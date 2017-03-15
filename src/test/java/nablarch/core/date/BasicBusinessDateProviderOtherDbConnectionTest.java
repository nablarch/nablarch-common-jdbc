package nablarch.core.date;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by IntelliJ IDEA.
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicBusinessDateProviderOtherDbConnectionTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/date/BasicBusinessDateProvider-test.xml");

    @BeforeClass
    public static void setUpClass() throws SQLException {
        VariousDbTestHelper.createTable(BusinessDate.class);
    }

    /**
     * コネクション名を設定しない場合のテスト。
     * <p/>
     * デフォルトのコネクション名を使用して、日付取得処理が行われること。
     * 既に開始されているコネクションを使用して日付を取得できること。
     */
    @Test
    public void testDefaultConnectionNamec() {

        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20120101"),
                new BusinessDate("02", "20130101"),
                new BusinessDate("03", "20140101")
        );

        BasicBusinessDateProvider basicBusinessDateProvider = new BasicBusinessDateProvider();

        basicBusinessDateProvider.setTableName("BUSINESS_DATE");
        basicBusinessDateProvider.setSegmentColumnName("SEGMENT");
        basicBusinessDateProvider.setDateColumnName("BIZ_DATE");
        basicBusinessDateProvider.setDefaultSegment("02");
        basicBusinessDateProvider.setCacheEnabled(false);
        basicBusinessDateProvider.initialize();

        SimpleDbTransactionManager transaction = repositoryResource.getComponent(
                "transaction");

        transaction.beginTransaction();
        try {
            assertThat("デフォルトのセグメント", basicBusinessDateProvider.getDate(), is(
                    "20130101"));
            assertThat("明示的にセグメントを指定した場合", basicBusinessDateProvider.getDate(
                    "00"), is("20110101"));
            assertThat("全データの取得", basicBusinessDateProvider.getAllDate(), is(
                    (Map) new HashMap<String, String>() {
                        {
                            put("00", "20110101");
                            put("01", "20120101");
                            put("02", "20130101");
                            put("03", "20140101");
                        }
                    }
            ));
            basicBusinessDateProvider.setDate("00", "21000101");
            assertThat(basicBusinessDateProvider.getDate("00"), is("21000101"));
        } finally {
            transaction.endTransaction();
        }
    }

    /**
     * コネクション名を設定する場合のテスト。
     * <p/>
     * 指定したコネクション名のトランザクションを使用して日付が取得出来ることを確認する。
     */
    @Test
    public void testSettingConnectionName() {

        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20120101"),
                new BusinessDate("02", "20130101"),
                new BusinessDate("03", "20140101")
        );

        BasicBusinessDateProvider basicBusinessDateProvider = new BasicBusinessDateProvider();

        basicBusinessDateProvider.setTableName("BUSINESS_DATE");
        basicBusinessDateProvider.setSegmentColumnName("SEGMENT");
        basicBusinessDateProvider.setDateColumnName("BIZ_DATE");
        basicBusinessDateProvider.setDefaultSegment("02");
        basicBusinessDateProvider.setCacheEnabled(false);
        basicBusinessDateProvider.setDbTransactionName("hogehoge");
        basicBusinessDateProvider.initialize();

        SimpleDbTransactionManager transaction = SystemRepository.get(
                "hogehogeTransaction");

        assertThat("トランザクション名は、ターゲットクラスに設定した値と一致していること",
                transaction.getDbTransactionName(), is("hogehoge"));

        transaction.beginTransaction();
        try {
            assertThat("デフォルトのセグメント", basicBusinessDateProvider.getDate(), is(
                    "20130101"));
            assertThat("明示的にセグメントを指定した場合", basicBusinessDateProvider.getDate(
                    "00"), is("20110101"));
            assertThat("全データの取得", basicBusinessDateProvider.getAllDate(), is(
                    (Map) new HashMap<String, String>() {
                        {
                            put("00", "20110101");
                            put("01", "20120101");
                            put("02", "20130101");
                            put("03", "20140101");
                        }
                    }
            ));
            basicBusinessDateProvider.setDate("00", "21000101");
            assertThat(basicBusinessDateProvider.getDate("00"), is("21000101"));
        } finally {
            transaction.endTransaction();
        }
    }

    /**
     * コネクション(トランザクション)が存在しない場合のテスト。
     * <p/>
     * コネクションが存在しない場合、日付取得時のみトランザクションを開始し値の取得が出来ることを確認する。
     */
    @Test
    public void testUnConnection() {

        VariousDbTestHelper.setUpTable(
                new BusinessDate("00", "20110101"),
                new BusinessDate("01", "20120101"),
                new BusinessDate("02", "20130101"),
                new BusinessDate("03", "20140101")
        );

        BasicBusinessDateProvider basicBusinessDateProvider = new BasicBusinessDateProvider();

        basicBusinessDateProvider.setTableName("BUSINESS_DATE");
        basicBusinessDateProvider.setSegmentColumnName("SEGMENT");
        basicBusinessDateProvider.setDateColumnName("BIZ_DATE");
        basicBusinessDateProvider.setDefaultSegment("02");
        basicBusinessDateProvider.setCacheEnabled(false);
        basicBusinessDateProvider.setDbTransactionName("unTransaction");
        basicBusinessDateProvider.initialize();

        SimpleDbTransactionManager transaction = SystemRepository.get(
                "transaction");
        basicBusinessDateProvider.setDbTransactionManager(transaction);
        assertThat(DbConnectionContext.containConnection("unTransaction"),
                is(false));

        // トランザクションを開始せずに日付取得処理を呼び出し
        assertThat("デフォルトのセグメント", basicBusinessDateProvider.getDate(),
                is("20130101"));
        assertThat("明示的にセグメントを指定した場合", basicBusinessDateProvider.getDate("00"),
                is("20110101"));
        assertThat("全データの取得", basicBusinessDateProvider.getAllDate(),
                is((Map) new HashMap<String, String>() {
                            {
                                put("00", "20110101");
                                put("01", "20120101");
                                put("02", "20130101");
                                put("03", "20140101");
                            }
                        }
                ));
        basicBusinessDateProvider.setDate("00", "21000101");
        assertThat(basicBusinessDateProvider.getDate("00"), is("21000101"));
    }

}
