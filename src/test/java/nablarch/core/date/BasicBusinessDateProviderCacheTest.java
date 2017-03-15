package nablarch.core.date;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.ThreadContext;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.ObjectLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link nablarch.core.date.BasicBusinessDateProvider}のテストクラス。
 * <p/>
 * 本テストクラスでは、キャッシュ設定({@link BasicBusinessDateProvider#setCacheEnabled(boolean)}を
 * オンにした状態で実施っする。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicBusinessDateProviderCacheTest {

    /** テスト対象クラス */
    private BasicBusinessDateProvider target;

    /** FW用connection */
    private SimpleDbTransactionManager db;

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/date/BasicBusinessDateProvider-test.xml");

    /**
     * テスト実施前準備
     *
     * @throws java.sql.SQLException SQL例外
     */
    @BeforeClass
    public static void classSetUp() throws SQLException {
        VariousDbTestHelper.createTable(BusinessDate.class);
    }

    @Before
    public void before() {
        // テスト対象クラスの初期化
        target = repositoryResource.getComponent("businessDateProvider");
        target.initialize();
        target.setCacheEnabled(true);
        db = SystemRepository.get("tran");
        db.beginTransaction();
        ThreadContext.clear();
    }

    @After
    public void after() {
        db.endTransaction();
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate()}のテスト。
     * 日付を取得後に、DBの値を変更しても前回取得した日付が取得できることを確認する。
     */
    @Test
    public void testGetDate() {
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );

        assertEquals("20110101", target.getDate());

        // 日付を変更する。
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110102"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );
        assertEquals("前回取得された日付が取得されること。", "20110101", target.getDate());

        target.setCacheEnabled(false);
        assertEquals("キャッシュなしに変更したので、変更した日付が取得されること。", "20110102",
                target.getDate());
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate(String)}のテスト
     * 日付を取得後に、DBの値を変更しても前回取得した日付が取得できることを確認する。
     */
    @Test
    public void testGetDateBySegment() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );
        assertEquals("20110301", target.getDate("02"));

        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110501")
        );
        assertEquals("変更前の日付が取得されること。", "20110301", target.getDate("02"));

        // キャッシュをなしに変更
        target.setCacheEnabled(false);
        assertEquals("キャッシュなしに変更したので、変更した日付が取得されること。", "20110501",
                target.getDate("02"));
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate(String)}のテスト<br>
     * 例外発生
     */
    @Test
    public void testGetDateErr() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );

        try {
            target.getDate("03");
            fail();
        } catch (IllegalStateException e) {
            assertEquals("segment was not found. segment:03.", e.getMessage());
        }

    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate(String)}のテスト<br>
     * 業務日付テーブルがからのため例外発生
     */
    @Test
    public void testGetDateErr2() {
        VariousDbTestHelper.delete(BusinessDate.class);
        try {
            target.getDate("03");
            fail("");
        } catch (IllegalStateException e) {
            assertEquals("business date was not registered.", e.getMessage());
        }

    }

    /** {@link nablarch.core.date.BasicBusinessDateProvider#getAllDate()}のテスト */
    @Test
    public void testGetAllDate() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );

        // 想定値の作成
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("00", "20110101");
        expected.put("01", "20110201");
        expected.put("02", "20110301");

        Map<String, String> actual = target.getAllDate();
        assertEquals(expected, actual);

        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110102"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110302")
        );

        assertEquals("前回取得された日付が取得されること。", expected, target.getAllDate());

        target.setCacheEnabled(false);
        expected.put("00", "20110102");
        expected.put("01", "20110202");
        expected.put("02", "20110302");
        assertEquals("キャッシュを無効にしたので最新データが取得されること。", expected,
                target.getAllDate());
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getAllDate()}のテスト<br>
     * 例外発生
     */
    @Test
    public void testGetAllDateErr() {
        VariousDbTestHelper.delete(BusinessDate.class);
        try {
            target.getAllDate();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("business date was not registered.", e.getMessage());
        }
    }

    /** {@link nablarch.core.date.BasicBusinessDateProvider#setDate(String, String)}のテスト */
    @Test
    public void testSetDate() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        //**********************************************************************
        // 更新前の情報を比較
        //**********************************************************************
        Map<String, String> allDate = target.getAllDate();

        // 期待値の生成
        Map<String, String> expected = new HashMap<String, String>();
        expected.put("00", "20110101");
        expected.put("01", "20110202");
        expected.put("02", "20110303");

        assertThat(allDate, is(expected));

        //**********************************************************************
        // 区分:00の日付を更新
        //**********************************************************************
        String date = "20501231";
        target.setDate("00", date);
        db.commitTransaction();

        //**********************************************************************
        // 更新結果のアサート
        //**********************************************************************
        // 更新対象区分のBIZ_DATEが更新されていることを検証
        List<BusinessDate> rows = VariousDbTestHelper.findAll(BusinessDate.class);
        /*
          SELECT SEGMENT, BIZ_DATE FROM BUSINESS_DATE
          */

        Map<String, String> actual = new HashMap<String, String>();
        for (BusinessDate row : rows) {
            actual.put(row.segment,
                    row.bizDate);
        }
        // 00の期待値を更新した日付に変更
        expected.put("00", "20501231");

        assertThat(actual, is(expected));

        //**********************************************************************
        // 再度getAllDateを呼び出した場合は、キャッシュされた値
        // (更新前の値)が取得できる事。
        //**********************************************************************
        allDate = target.getAllDate();

        // 期待値の日付を更新前の値に戻す。
        expected.put("00", "20110101");
        assertThat(allDate, is(expected));

    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
     * 不正パラメータ(null)のチェック
     */
    @Test
    public void testSetDateNullCheck() {
        // segment:null, date:"20500101"
        try {
            target.setDate(null, "20500101");
            // エラーが発生しなかったらテスト失敗
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("segment was null.", e.getMessage());
        }

        // segment:"00", date:null
        try {
            target.setDate("00", null);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("date was null.", e.getMessage());
        }
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
     * 不正パラメータ(空文字)のチェック
     */
    @Test
    public void testSetDateEmptyCheck() {
        // segment:"", date:"20500101"
        try {
            target.setDate("", "20500101");
            // エラーが発生しなかったらテスト失敗
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("segment was empty.", e.getMessage());
        }

        // segment:"00", date:""
        try {
            target.setDate("00", "");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("date was empty.", e.getMessage());
        }
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
     * 日付のフォーマットチェック
     */
    @Test
    public void testSetDateFormatCheck() {
        // segment:"00", date:"2050/01/01"
        String[] params1 = new String[] {"00", "aaaaaaaa"};
        // segment:"00", date:"2050/01/01"
        String[] params2 = new String[] {"00", "2050/1/1"};
        // segment:"00", date:"2050/01/01"
        String[] params3 = new String[] {"00", "20509999"};

        List<String[]> cases = new ArrayList<String[]>();
        cases.add(params1);
        cases.add(params2);
        cases.add(params3);

        for (String[] params : cases) {
            try {
                target.setDate(params[0], params[1]);
                // エラーが発生しなかったらテスト失敗
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals(
                        "date was not formatted 'yyyyMMdd' or non existent date. date:"
                                + params[1] + ".", e.getMessage());
            }
        }
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
     * 存在しない区分値の日付を更新しようとする
     */
    @Test
    public void testSetDateUpdateFail() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        // テーブルに存在しない区分値
        String segment = "99";
        String date = "20501231";
        try {
            target.setDate(segment, date);
            // エラーが発生しなかったらテスト失敗
            fail();
        } catch (IllegalStateException e) {
            assertEquals("segment was not found. segment:" + segment + ".",
                    e.getMessage());
        }
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate(String)}のテスト。
     * システムリポジトリに日付の上書き設定があった場合のテスト。
     */
    @Test
    public void testGetDateForSystemRepository() {

        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        assertThat(target.getDate("00"), is("20110101"));
        assertThat(target.getDate("01"), is("20110202"));
        assertThat(target.getDate("02"), is("20110303"));

        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("BasicBusinessDateProvider.00", "20990101");
                map.put("BasicBusinessDateProvider.01", "20990102");
                map.put("BasicBusinessDateProvider.02", "20990103");
                return map;
            }
        });

        assertThat(target.getDate("00"), is("20990101"));
        assertThat(target.getDate("01"), is("20990102"));
        assertThat(target.getDate("02"), is("20990103"));

        // 不正な日付形式の場合
        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("BasicBusinessDateProvider.00", "20990100");
                return map;
            }
        });

        try {
            target.getDate("00");
            fail("does not run.");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertThat("不正な日付のためエラーがとなる。", msg,
                    containsString("business date in system repository was invalid."));
            assertThat(msg,
                    containsString("date=[20990100] segment=[00]"));
        }
    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate()}のテスト。
     * システムリポジトリに日付の上書き設定があった場合のテスト。
     */
    @Test
    public void testGetDateForSystemRepository2() {

        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        assertThat(target.getDate(), is("20110101"));

        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("BasicBusinessDateProvider.00", "20990101");
                map.put("BasicBusinessDateProvider.01", "20990102");
                map.put("BasicBusinessDateProvider.02", "20990103");
                return map;
            }
        });

        assertThat(target.getDate(), is("20990101"));

    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getAllDate()}のテスト
     * リポジトリに日付の上書き設定がある場合、上書きされた日付が取得されること。
     */
    @Test
    public void testGetAllDateForSystemRepository() {

        // DBの設定（上書きを行わない場合は、本DBの値が取得できる)
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        // 上書きする値をリポジトリに登録
        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("BasicBusinessDateProvider.00", "20110711");
                data.put("BasicBusinessDateProvider.01", "20110712");
                data.put("BasicBusinessDateProvider.02", "20110713");
                data.put("BasicBusinessDateProvider.03", "20110713");
                return data;
            }
        });

        Map<String, String> allDate = target.getAllDate();
        assertThat(allDate.get("00"), is("20110711"));
        assertThat(allDate.get("01"), is("20110712"));
        assertThat(allDate.get("02"), is("20110713"));

    }
}
