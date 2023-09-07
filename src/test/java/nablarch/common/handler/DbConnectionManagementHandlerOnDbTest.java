package nablarch.common.handler;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.transaction.JdbcTransactionFactory;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

/**
 * {@link DbConnectionManagementHandler}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class DbConnectionManagementHandlerOnDbTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/common/handler/DbConnectionManagementHandler.xml");

    /** テスト用のトランザクション名 */
    private static final String TRANSACTION_NAME = "TEST_DB_NAME";

    @Before
    public void setUp() throws Exception {
        OnMemoryLogWriter.clear();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト
     *
     * @throws Exception Exception
     */
    @Test
    public void normalCase() throws Exception {
        // テストデータの準備
        setUpTable();

        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        // トランザクション制御用のハンドラを追加
        TransactionManagementHandler transaction = new TransactionManagementHandler();
        transaction.setTransactionFactory(new JdbcTransactionFactory());
        transaction.setTransactionName(TRANSACTION_NAME);
        handlers.add(transaction);

        // テストで使用するテーブルのデータを削除するハンドラ
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                VariousDbTestHelper.delete(TestTable.class);
                return new Object();
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // 想定通りDB接続ができ、対象テーブルのデータが削除されていることを確認
        List<TestTable> result = VariousDbTestHelper.findAll(TestTable.class);
        assertThat("レコードが1件存在すること", result.size(), is(1));

        // XMLからテスト対象のクラスの読み込み
        DbConnectionManagementHandler handler = repositoryResource.getComponent("DbConnectionManagementHandler");
        handler.handle(null, context);

        // 想定通りDB接続ができ、対象テーブルのデータが削除されていることを確認
        result = VariousDbTestHelper.findAll(TestTable.class);
        assertThat("レコードが削除されていること", result.size(), is(0));

        // スレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラでRuntimeExceptionが発生した場合<br/>
     * 期待値:ハンドラで発生したRuntimeExceptionがthrowされてくる<br/>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_RuntimeException() throws Exception {
        // テストデータの準備
        setUpTable();

        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        // トランザクション制御用のハンドラを追加
        TransactionManagementHandler transaction = new TransactionManagementHandler();
        transaction.setTransactionFactory(new JdbcTransactionFactory());
        transaction.setTransactionName(TRANSACTION_NAME);
        handlers.add(transaction);

        // 例外を発生させるハンドラ
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new RuntimeException("error!!");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // XMLからテスト対象のクラスの読み込み
        DbConnectionManagementHandler handler = repositoryResource.getComponent("DbConnectionManagementHandler");
        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("error!!"));
        }

        assertWarnLogCountIs(0);

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:DbConnectionManagementHandlerのfinally句でRuntimeExceptionが発生した場合。<br/>
     * 期待値:例外がスローされず、正常終了する。<br/>
     *
     * @throws Exception Exception
     */
    @Test
    public void finally_RuntimeException() throws Exception {
        // テストデータの準備
        setUpTable();

        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                return new Object();
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new RuntimeException("terminate error!!!");
        }};

        handler.handle(null, context);

        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.RuntimeException: terminate error!!");

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラでErrorが発生した場合<br/>
     * 期待値:ハンドラで発生したErrorがthrowされてくる。<br/>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_Error() throws Exception {
        // テストデータの準備
        setUpTable();

        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new OutOfMemoryError("hoge");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // テスト対象クラスを生成
        DbConnectionManagementHandler handler = repositoryResource.getComponent("DbConnectionManagementHandler");
        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (OutOfMemoryError e) {
            assertThat(e.getMessage(), is("hoge"));
        }

        assertWarnLogCountIs(0);

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:DbConnectionManagementHandlerのfinally句でErrorが発生した場合。<br/>
     * 期待値:Errorはスローされず正常終了する。<br/>
     *
     * @throws Exception Exception
     */
    @Test
    public void finally_Error() throws Exception {
        // テストデータの準備
        setUpTable();

        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                return new Object();
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");

        // テスト対象クラスを生成
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            final TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new Error("error.");
        }};

        handler.handle(null, context);

        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.Error: error.");

        // スレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラと、DbConnectionManagementHandlerのfinally句でRuntimeExceptionが発生した場合。<br/>
     * 期待値:
     * <ol>
     * <li>ハンドラで発生した例外が送出されてくることを確認する。</li>
     * <li>finally句で発生した例外はワーニングレベルでログ出力されていることを確認する。</li>
     * </ol>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_finally_RuntimeException() throws Exception {
        // テストデータの準備
        setUpTable();

        // 元例外を送出するハンドラを追加
        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new NullPointerException("runtime error.");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);


        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            final TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new RuntimeException("terminate error!!!");
        }};

        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), is("runtime error."));
        }

        // 元例外をアサート
        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.RuntimeException: terminate error!!!");

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラでErrorが発生し、DbConnectionManagementHandlerのfinally句でRuntimeExceptionが発生した場合。<br/>
     * 期待値:<br/>
     * <ol>
     * <li>ハンドラで発生した例外が送出されてくることを確認する。</li>
     * <li>finally句で発生した例外はワーニングレベルでログ出力されていることを確認する。</li>
     * </ol>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_Error_finally_RuntimeException() throws Exception {
        // テストデータの準備
        setUpTable();

        // 元例外を送出するハンドラを追加
        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new OutOfMemoryError("out of memory error.");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);


        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            final TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new RuntimeException("terminate error!!!");
        }};

        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (OutOfMemoryError e) {
            assertThat(e.getMessage(), is("out of memory error."));
        }

        // 元例外をアサート
        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.RuntimeException: terminate error!!!");

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラと、DbConnectionManagementHandlerのfinally句でErrorが発生した場合。<br/>
     * 期待値:<br/>
     * <ol>
     * <li>ハンドラで発生した例外が送出されてくることを確認する。</li>
     * <li>finally句で発生した例外はワーニングレベルでログ出力されていることを確認する。</li>
     * </ol>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_finally_Error() throws Exception {
        // テストデータの準備
        setUpTable();

        // 元例外を送出するハンドラを追加
        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new ClassFormatError("class format error.");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            final TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new Error("error.");
        }};

        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (ClassFormatError e) {
            assertThat(e.getMessage(), is("class format error."));
        }

        // 元例外をアサート
        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.Error: error.");

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * {@link DbConnectionManagementHandler#handle(Object, nablarch.fw.ExecutionContext)}の異常系テスト。
     * <br/>
     * ケース内容:ハンドラでRuntimeException、DbConnectionManagementHandlerのfinally句でErrorが発生した場合。<br/>
     * 期待値:<br/>
     * <ol>
     * <li>ハンドラで発生した例外が送出されてくることを確認する。</li>
     * <li>finally句で発生した例外はワーニングレベルでログ出力されていることを確認する。</li>
     * </ol>
     *
     * @throws Exception Exception
     */
    @Test
    public void handle_RuntimeException_finally_Error() throws Exception {
        // テストデータの準備
        setUpTable();

        // 元例外を送出するハンドラを追加
        List<Handler<?, ?>> handlers = new ArrayList<Handler<?, ?>>();
        handlers.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                throw new IndexOutOfBoundsException(
                        "java.lang.IndexOutOfBoundsException");
            }
        });
        ExecutionContext context = new ExecutionContext();
        context.addHandlers(handlers);

        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        new Expectations(factory) {{
            final TransactionManagerConnection connection = factory.getConnection(TRANSACTION_NAME);
            connection.terminate();
            result = new Error("error.");
        }};

        try {
            handler.handle(null, context);
            fail("does not run.");
        } catch (IndexOutOfBoundsException e) {
            assertThat(e.getMessage(), is("java.lang.IndexOutOfBoundsException"));
        }

        // 元例外をアサート
        assertWarnLogCountIs(1);
        assertWarnLog("java.lang.Error: error.");

        // 例外が発生してもスレッドコンテキストから削除されていることを確認
        assertRemoveConnection();
    }

    /**
     * 既に使用されているコネクション名が指定された場合は、既に使用されています例外が発生すること
     * また、この場合には{@link ConnectionFactory#getConnection(String)}が呼び出されないことを検証する。
     */
    @Test
    public void connectionNameAlreadyUsed(@Mocked TransactionManagerConnection mockConnection) throws Exception {

        ExecutionContext context = new ExecutionContext();

        // テスト対象クラスを生成
        final ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        new Expectations(factory) {{
            factory.getConnection(TRANSACTION_NAME);
            minTimes = 0;
        }};


        DbConnectionManagementHandler handler = new DbConnectionManagementHandler();
        handler.setConnectionFactory(factory);
        handler.setConnectionName(TRANSACTION_NAME);

        DbConnectionContext.setConnection(TRANSACTION_NAME, mockConnection);
        try {
            handler.handle(null, context);
            fail("ここはとおらない");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(
                    MessageFormat.format("The specified database connection name is already used. connection name=[{0}]",
                            TRANSACTION_NAME)));
        } finally {
            DbConnectionContext.removeConnection(TRANSACTION_NAME);
        }

        new Verifications() {{
            // 既に登録済みのコネクション名なので新たなコネクションを取得しないことを検証する。
            factory.getConnection(anyString);
            times = 0;
        }};
    }

    /**
     * ワーニングログをアサートする。
     *
     * @param message ログのメッセージ
     */
    private static void assertWarnLog(String message) {
        List<String> log = OnMemoryLogWriter.getMessages("writer.memory");
        boolean writeLog = false;
        for (String logMessage : log) {
            String str = logMessage.replaceAll("[\\r\\n]", "");
            if (str.matches(
                    "^.*WARN.*DbConnectionManagementHandler.*failed in the "
                            + "application process\\..*" + message + ".*$")) {
                writeLog = true;
                break;
            }
        }
        assertThat("元例外がWARNレベルでログに出力されていること", writeLog, is(true));
    }


    /**
     * ワーニングログの件数をアサートする。
     *
     * @param count ログのカウント
     */
    private static void assertWarnLogCountIs(int count){
        List<String> log = OnMemoryLogWriter.getMessages("writer.memory");
        int warnCount = 0;
        for (String logMessage : log) {
            String str = logMessage.replaceAll("[\\r\\n]", "");
            if (str.matches(
                    "^.*WARN.*failed in the "
                            + "application process\\..*$")) {
                warnCount++;
            }
        }
        assertThat(warnCount, is(count));
    }

    /** {@link DbConnectionContext}からコネクションが削除されていることを確認する。 */
    private static void assertRemoveConnection() {
        try {
            DbConnectionContext.getConnection(TRANSACTION_NAME);
            fail("does not run.");
        } catch (IllegalArgumentException e) {
            // NOP
        }
    }

    /** テスト用のテーブルをセットアップ。 */
    private void setUpTable() {
        VariousDbTestHelper.createTable(TestTable.class);
        VariousDbTestHelper.setUpTable(new TestTable("00001"));
    }
}
