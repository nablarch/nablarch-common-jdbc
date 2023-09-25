package nablarch.common.handler;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.StringUtil;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.InboundHandleable;
import nablarch.fw.OutboundHandleable;
import nablarch.fw.Result;

/**
 * 後続ハンドラの処理で必要となる、データベース接続オブジェクトを
 * スレッドローカル変数上で管理するハンドラ。
 * <pre>
 * デフォルトの設定では、トランザクションが暗黙的に使用する接続名
 * ({@link TransactionContext#DEFAULT_TRANSACTION_CONTEXT_KEY})
 * に対して接続オブジェクトを登録する。
 * 接続名を明示的に指定する場合は、属性dbConnectionNameにその値を設定する。<br/>
 * &lt;!-- 設定例 --&gt;
 * &lt;component class="nablarch.common.handler.DbConnectionManagementHandler"&gt;
 *      &lt;!-- DbConnectionFactory --&gt;
 *      &lt;property name="dbConnectionFactory" ref="dbConnectionFactory"/&gt;
 *      &lt;!-- 追加するデータベース接続オブジェクトの名称 --&gt;
 *      &lt;property name="dbConnectionName" value="db"/&gt;
 * &lt;/component&gt;
 * </pre>
 *
 * @author Iwauo Tajima
 */
public class DbConnectionManagementHandler implements Handler<Object, Object>, InboundHandleable, OutboundHandleable {

    /** データベース接続オブジェクトを取得するためのファクトリ */
    private ConnectionFactory connectionFactory;

    /** このハンドラが生成するコネクションの登録名 */
    private String connectionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /** Logger */
    private static final Logger LOGGER = LoggerManager.get(
            DbConnectionManagementHandler.class);

    /**
     * データベース接続オブジェクトを生成するためのファクトリを設定する。
     *
     * @param connectionFactory データベース接続オブジェクトを生成するためのファクトリ
     * @return このハンドラ自体
     */
    public DbConnectionManagementHandler setConnectionFactory(ConnectionFactory connectionFactory) {
        assert connectionFactory != null;
        this.connectionFactory = connectionFactory;
        return this;
    }

    /**
     * データベース接続のスレッドコンテキスト上の登録名を設定する。
     * <pre>
     * デフォルトでは既定のトランザクション名
     * ({@link TransactionContext#DEFAULT_TRANSACTION_CONTEXT_KEY})を使用する。
     * </pre>
     *
     * @param connectionName データベース接続のスレッドコンテキスト上の登録名
     */
    public void setConnectionName(String connectionName) {
        assert !StringUtil.isNullOrEmpty(connectionName);
        this.connectionName = connectionName;
    }

    /**
     * {@inheritDoc}
     * <pre>
     * このクラスの実装では後続ハンドラに対する処理委譲の前後に、
     * データベース接続オブジェクトの初期化と終了の処理をそれぞれ行う。
     * </pre>
     */
    public Object handle(Object inputData, ExecutionContext ctx) {

        before();

        Throwable throwable = null;
        try {
            return ctx.handleNext(inputData);
        } catch (RuntimeException e) {
            throwable = e;
            throw e;
        } catch (Error e) {
            throwable = e;
            throw e;
        } finally {
            try {
                after();
            } catch (RuntimeException e) {
                writeWarnLog(e);
            } catch (Error e) {
                if(throwable instanceof Error){
                    writeWarnLog(e);
                    throw (Error)throwable;
                }
                if(throwable != null) {
                    writeWarnLog(throwable);
                }
                throw e;
            }
        }
    }

    /**
     * 往路処理を行う。
     * <p/>
     * {@link ConnectionFactory}から{@link TransactionManagerConnection}を取得し、
     * {@link DbConnectionContext}に設定する。
     */
    public void before() {
        if (DbConnectionContext.containConnection(connectionName)) {
            throw new IllegalStateException(
                    "The specified database connection name is already used. connection name=[" + connectionName + ']');
        }
        DbConnectionContext.setConnection(connectionName, connectionFactory.getConnection(connectionName));
    }

    /**
     * 復路処理を行う。
     *
     * <p>
     * {@link DbConnectionContext}からデータベース接続を削除し、リソースの開放処理を行う。
     */
    public void after() {
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection(connectionName);
        DbConnectionContext.removeConnection(connectionName);
        connection.terminate();
    }

    /**
     * ワーニングログの出力を行う。
     * <br/>
     *
     * @param throwable ログに出力する例外
     */
    private static void writeWarnLog(Throwable throwable) {
        if (throwable != null) {
            LOGGER.logWarn("DbConnectionManagementHandler#handle failed in the application process.", throwable);
        }
    }

    @Override
    public Result handleInbound(ExecutionContext context) {
        before();
        return new Result.Success();
    }

    @Override
    public Result handleOutbound(ExecutionContext context) {
        after();
        return new Result.Success();
    }
}

