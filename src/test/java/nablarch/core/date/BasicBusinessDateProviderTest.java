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
 * {@link BasicBusinessDateProvider}のテストクラス
 *
 * @author Miki Habu
 */
@RunWith(DatabaseTestRunner.class)
public class BasicBusinessDateProviderTest {

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
     * @throws SQLException SQL例外
     */
    @BeforeClass
    public static void classSetUp() throws SQLException {
        VariousDbTestHelper.createTable(BusinessDate.class);
        // テスト対象クラスの初期化
    }

    @Before
    public void setUp() throws Exception {
        target = repositoryResource.getComponent("businessDateProvider");
        target.setCacheEnabled(false);
        target.initialize();
        db = repositoryResource.getComponent("transaction");
        db.beginTransaction();
    }

    /** テストケースごとの事後処理 */
    @After
    public void after() {
        db.endTransaction();
    }

    /** {@link BasicBusinessDateProvider#getDate()}のテスト */
    @Test
    public void testGetDate() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );

        String actual = target.getDate();
        assertEquals("20110101", actual);
    }

    /**
     * {@link BasicBusinessDateProvider#getDate()}のテスト。<br/>
     * 取得した業務日付が不正な場合、例外が発生すること。
     */
    @Test
    public void testGetDateFail() {
        // リポジトリに不正な業務日付を登録する
        repositoryResource.addComponent("BasicBusinessDateProvider.99", "200111");

        // 業務日付がフォーマットに適合しない場合、例外が発生すること
        try {
            target.getDate("99");
            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString(
                    "business date in system repository was invalid. date=[200111] segment=[99]"));
        }
    }


    /** {@link BasicBusinessDateProvider#getDate(String)}のテスト */
    @Test
    public void testGetDateBySegment() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110201"),
                new BusinessDate("02", "20110301")
        );

        String actual = target.getDate("01");
        assertEquals("20110201", actual);
    }

    /**
     * {@link BasicBusinessDateProvider#getDate(String)}のテスト<br>
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

    /** {@link BasicBusinessDateProvider#getAllDate()}のテスト */
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
    }

    /**
     * {@link BasicBusinessDateProvider#getAllDate()}のテスト<br>
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

    /**
     * {@link BasicBusinessDateProvider#getAllDate()}のテスト<br>
     * 業務日付テーブルのレコードと同じセグメントの業務日付がシステムリポジトリに登録されている場合で、
     * その業務日付がフォーマットに適合しない場合、例外が発生すること。
     */
    @Test
    public void testGetAllDateFail() {
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("99", "20110102")
        );

        // リポジトリに不正な業務日付を登録する(こちらがテーブルより優先される）。
        repositoryResource.addComponent("BasicBusinessDateProvider.99", "200111");

        // 業務日付がフォーマットに適合しない場合、例外が発生すること
        try {
            target.getAllDate();
            fail();
        } catch (IllegalStateException e) {
            String msg = e.getMessage();
            assertThat(msg, containsString(
                    "business date in system repository was invalid."));
            assertThat(msg, containsString(
                    "date=[200111]"));
            assertThat(msg, containsString(
                    "segment=[99]"));
        }
    }


    /** {@link BasicBusinessDateProvider#setDate(String, String)}のテスト */
    @Test
    public void testSetDate() {
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        String date = "20501231";
        target.setDate("00", date);
        db.commitTransaction();

        // 更新対象区分のBIZ_DATEが更新されていることを検証
        BusinessDate row = VariousDbTestHelper.findById(BusinessDate.class, "00");
        assertEquals(date, row.bizDate);

        // 更新対象ではない区分のBIZ_DATEが更新されていないことを検証
        row = VariousDbTestHelper.findById(BusinessDate.class, "01");
        assertEquals("20110202", row.bizDate);

        // 02
        row = VariousDbTestHelper.findById(BusinessDate.class, "02");
        assertEquals("20110303", row.bizDate);
    }

    /**
     * {@link BasicBusinessDateProvider#setDate(String, String)}のテスト。<br/>
     * 設定する日付が不正な場合、例外が発生すること。
     */
    @Test
    public void testSetDateFail() {
        // DBの設定
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101")
        );

        // 桁数不足の時、例外が発生すること
        try {
            target.setDate("00", "2000111");
            fail();
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertThat(msg, containsString(
                    "date was not formatted 'yyyyMMdd' or non existent date."));
            assertThat(msg, containsString("date:2000111"));
        }

        try {
            target.setDate("00", "200011111");
            fail();
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertThat(msg, containsString(
                    "date was not formatted 'yyyyMMdd' or non existent date."));
            assertThat(msg, containsString("date:200011111"));
        }
    }

    /**
     * {@link BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
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
     * {@link BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
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
     * {@link BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
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
     * {@link BasicBusinessDateProvider#setDate(String, String)}のテスト<br>
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
     * {@link BasicBusinessDateProvider#getDate(String)}のテスト。
     * <br/>
     * リポジトリに日付の上書き設定がある場合、上書きされた日付が取得されること。
     */
    @Test
    public void testGetDateForSystemRepository() {

        // DBの設定（上書きを行わない場合は、本DBの値が取得できる)
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        assertThat("DBの日付が取得できること。", target.getDate("00"), is("20110101"));

        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("BasicBusinessDateProvider.00", "20110711");
                data.put("BasicBusinessDateProvider.04", "20110801");
                return data;
            }
        });

        assertThat("上書きした日付が取得できること。", target.getDate("00"), is("20110711"));
        assertThat("２どめでも同じ日付が取れる。", target.getDate("00"), is("20110711"));
        assertThat("DBに存在しないが上書き設定しているので、値が取得できること。",
                target.getDate("04"), is("20110801"));

        // 不正な形式の日付を指定
        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("BasicBusinessDateProvider.00", "20111301");
                return data;
            }
        });

        try {
            target.getDate("00");
            fail("does not run.");
        } catch (Exception e) {
            String msg = e.getMessage();
            assertThat("不正な日付形式の為エラーとなる。", msg,
                    containsString("business date in system repository was invalid."));
            assertThat(msg,
                    containsString("date=[20111301] segment=[00]"));

        }

    }

    /**
     * {@link nablarch.core.date.BasicBusinessDateProvider#getDate()}のテスト。
     * <br/>
     * リポジトリに日付の上書き設定がある場合、上書きされた日付が取得されること。
     */
    @Test
    public void testGetDateForSystemRepository2() {

        // DBの設定（上書きを行わない場合は、本DBの値が取得できる)
        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20110202"),
                new BusinessDate("02", "20110303")
        );

        assertThat("デフォルトの区分は、00なので20110101が取得される。", target.getDate(), is(
                "20110101"));

        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("BasicBusinessDateProvider.00", "20110711");
                data.put("BasicBusinessDateProvider.04", "20110801");
                return data;
            }
        });

        assertThat("上書きした日付が取得できること。", target.getDate(), is("20110711"));
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

        Map<String, String> allDate = target.getAllDate();

        // 上書きされる前の値を比較
        assertThat(allDate.get("00"), is("20110101"));
        assertThat(allDate.get("01"), is("20110202"));
        assertThat(allDate.get("02"), is("20110303"));

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

        allDate = target.getAllDate();
        assertThat(allDate.get("00"), is("20110711"));
        assertThat(allDate.get("01"), is("20110712"));
        assertThat(allDate.get("02"), is("20110713"));

        // 不正な日付形式のケース
        SystemRepository.load(new ObjectLoader() {
            public Map<String, Object> load() {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("BasicBusinessDateProvider.00", "20111111"); // OK
                data.put("BasicBusinessDateProvider.01", "20110732"); // NG
                data.put("BasicBusinessDateProvider.02", "20110630"); // OK
                return data;
            }
        });

        try {
            target.getAllDate();
            fail("does not run.");
        } catch (IllegalStateException e) {
            String msg = e.getMessage();
            assertThat("不正な日付のためエラーがとなる。", msg,
                    containsString("business date in system repository was invalid."));
            assertThat(msg,
                    containsString("date=[20110732] segment=[01]"));
        }

    }

}
