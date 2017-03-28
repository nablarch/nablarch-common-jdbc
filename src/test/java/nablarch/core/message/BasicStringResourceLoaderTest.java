package nablarch.core.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Locale;

import org.hamcrest.CoreMatchers;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DatabaseTestRunner.class)
public class BasicStringResourceLoaderTest {

    /**
     * ロガー。
     */
    private static final Logger LOGGER = LoggerManager
            .get(BasicStringResourceLoaderTest.class);

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/message/message-loader-test.xml");

    @BeforeClass
    public static void classSetup() throws Exception {
        VariousDbTestHelper.createTable(TestMessage2.class);
    }

    @Test
    public void testGetValue() {

        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        BasicStringResource msg1;
        BasicStringResource msg2;
        Object key1;
        Object key2;
        try {
            msg1 = (BasicStringResource) stringResourceLoader.getValue("10001");
            msg2 = (BasicStringResource) stringResourceLoader.getValue("10002");
            key1 = stringResourceLoader.getId(msg1);
            key2 = stringResourceLoader.getId(msg2);

            // 対象データが存在しない場合
            StringResource nothing = stringResourceLoader.getValue("none");
            assertThat(nothing, CoreMatchers.nullValue());
        } finally {
        }

        assertEquals("10001", msg1.getId());
        assertEquals("メッセージ001", msg1.getValue(Locale.JAPANESE));
        assertEquals("Message001", msg1.getValue(Locale.ENGLISH));
        assertEquals("10002", msg2.getId());
        assertEquals("メッセージ002", msg2.getValue(Locale.JAPANESE));
        assertEquals("Message002", msg2.getValue(Locale.ENGLISH));

        assertEquals("10001", key1);
        assertEquals("10002", key2);

    }


    @Test
    public void testGetValueWithDbName() {

        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        BasicStringResource msg1;
        BasicStringResource msg2;
        Object key1;
        Object key2;
        try {
            msg1 = (BasicStringResource) stringResourceLoader.getValue("10001");
            msg2 = (BasicStringResource) stringResourceLoader.getValue("10002");
            key1 = stringResourceLoader.getId(msg1);
            key2 = stringResourceLoader.getId(msg2);
        } finally {
        }

        assertEquals("10001", msg1.getId());
        assertEquals("メッセージ001", msg1.getValue(Locale.JAPANESE));
        assertEquals("Message001", msg1.getValue(Locale.ENGLISH));
        assertEquals("10002", msg2.getId());
        assertEquals("メッセージ002", msg2.getValue(Locale.JAPANESE));
        assertEquals("Message002", msg2.getValue(Locale.ENGLISH));

        assertEquals("10001", key1);
        assertEquals("10002", key2);

    }


    @Test
    public void testGetValueWithoutTransaction() {

        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        BasicStringResource msg1;
        BasicStringResource msg2;
        Object key1;
        Object key2;
        msg1 = (BasicStringResource) stringResourceLoader.getValue("10001");
        msg2 = (BasicStringResource) stringResourceLoader.getValue("10002");
        key1 = stringResourceLoader.getId(msg1);
        key2 = stringResourceLoader.getId(msg2);

        boolean transactionEnded = !DbConnectionContext.containConnection(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

        assertEquals("10001", msg1.getId());
        assertEquals("メッセージ001", msg1.getValue(Locale.JAPANESE));
        assertEquals("Message001", msg1.getValue(Locale.ENGLISH));
        assertEquals("10002", msg2.getId());
        assertEquals("メッセージ002", msg2.getValue(Locale.JAPANESE));
        assertEquals("Message002", msg2.getValue(Locale.ENGLISH));

        assertEquals("10001", key1);
        assertEquals("10002", key2);

        assertTrue("コネクションは閉じているはず", transactionEnded);

    }

    @Test
    public void testNotImplementedMethods() {
        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        List<String> indexNames;
        List<StringResource> values;
        Object indexKey;
        try {
            BasicStringResource msg1 = (BasicStringResource) stringResourceLoader.getValue("10001");

            indexNames = stringResourceLoader.getIndexNames();
            values = stringResourceLoader.getValues("test", "");
            indexKey = stringResourceLoader.generateIndexKey("test", msg1);
        } finally {
        }


        assertTrue(values == null);
        assertTrue(indexNames == null);
        assertTrue(indexKey == null);
    }

    @Test
    public void testLoadAll() {

        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        List<StringResource> allMessages;
        BasicStringResource msg0;
        BasicStringResource msg1;
        allMessages = stringResourceLoader.loadAll();

        msg0 = (BasicStringResource) allMessages.get(0);
        msg1 = (BasicStringResource) allMessages.get(1);


        assertEquals(2, allMessages.size());
        assertEquals("10001", msg0.getId());
        assertEquals("メッセージ001", msg0.getValue(Locale.JAPANESE));
        assertEquals("Message001", msg0.getValue(Locale.ENGLISH));
        assertEquals("10002", msg1.getId());
        assertEquals("メッセージ002", msg1.getValue(Locale.JAPANESE));
        assertEquals("Message002", msg1.getValue(Locale.ENGLISH));
    }

    @Test
    public void testLoadAllWithDbName() {

        VariousDbTestHelper.setUpTable(
                new TestMessage2("10001", "ja", "メッセージ001"),
                new TestMessage2("10001", "en", "Message001"),
                new TestMessage2("10002", "ja", "メッセージ002"),
                new TestMessage2("10002", "en", "Message002")
        );

        BasicStringResourceLoader stringResourceLoader = repositoryResource.getComponentByType(
                BasicStringResourceLoader.class);
        stringResourceLoader.setDbManager((SimpleDbTransactionManager) repositoryResource.getComponent("dbManager"));

        List<StringResource> allMessages;
        BasicStringResource msg0;
        BasicStringResource msg1;
        allMessages = stringResourceLoader.loadAll();

        msg0 = (BasicStringResource) allMessages.get(0);
        msg1 = (BasicStringResource) allMessages.get(1);

        assertEquals(2, allMessages.size());
        assertEquals("10001", msg0.getId());
        assertEquals("メッセージ001", msg0.getValue(Locale.JAPANESE));
        assertEquals("Message001", msg0.getValue(Locale.ENGLISH));
        assertEquals("10002", msg1.getId());
        assertEquals("メッセージ002", msg1.getValue(Locale.JAPANESE));
        assertEquals("Message002", msg1.getValue(Locale.ENGLISH));

        try {
            msg1.getValue(Locale.CHINESE);
            fail("例外が発生するはず");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage()
                    .indexOf("10002") > 0);
        }
    }
}
