package nablarch.core.date;

import nablarch.core.util.DateUtil;
import nablarch.core.ThreadContext;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.core.repository.initialization.Initializable;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.StringUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 業務日付を提供するクラス。
 * <p/>
 * 本クラスでは、テーブルで管理されている業務日付を取得する機能を提供する。
 * <p/>
 * 業務日付の取得要求の都度データベースアクセスを行うとパフォーマンス上問題となる場合がある。
 * このため、{@link #cacheEnabled}プロパティを使用して業務日付のキャッシュ有無を設定でき、
 * データベースアクセスを最小限に抑えることが可能となっている。
 * <p/>
 * {@link #cacheEnabled}にtrueを設定するとキャッシュが有効になり、
 * 初回アクセス時にテーブルの情報が{@link ThreadContext}にキャッシュされる。
 * キャッシュされた値の生存期間は、{@link ThreadContext}がクリアされるか、
 * スレッドが破棄されるかのどちらかである。
 * <p/>
 * 例えば、画面オンライン処理の場合は、リクエスト受付時に{@link ThreadContext}の情報がクリアされるため、
 * 業務日付のキャッシュ有効期間はクライアントからの１リクエストを処理する間となる。
 * <p/>
 * 業務日付を管理するテーブルのレイアウトを以下に示す。
 * <table border="1">
 * <tr  bgcolor="#CCCCFF">
 * <th>カラム名</th>
 * <th>説明</th>
 * </tr>
 * <tr>
 * <td>区分</td>
 * <td>
 * 業務日付を特定するための区分<br/>
 * 例えば、画面処理とバッチ処理で日付の更新タイミングが異なる場合は、<br/>
 * 画面処理用とバッチ処理用の区分を設けて日付を管理すれば良い。
 * </td>
 * </tr>
 * <tr>
 * <td>日付</td>
 * <td>区分に対応する業務日付</td>
 * </tr>
 * </table>
 * <p/>
 * なお、本クラスでは特定の区分の業務日付をリポジトリ({@link SystemRepository})
 * に登録した日付で上書きする機能を提供する。
 * この機能は、バッチアプリケーションなどで指定した日付で業務処理を実行したい場合に使用する。<br/>
 * <b>リポジトリに登録された日付の形式が不正な場合、初回アクセス時に{@link RuntimeException}を送出する。</b>
 * <p/>
 * リポジトリには、下記形式で上書きを行いたい日付を登録すること。
 * <table border="1">
 * <tr  bgcolor="#CCCCFF">
 * <th>キー</th>
 * <th>値</th>
 * </tr>
 * <tr>
 * <td>BasicBusinessDateProvider.区分値</td>
 * <td>上書く日付</td>
 * </tr>
 * </table>
 * 以下に例を示す。:
 * <pre>
 * {@code
 * 区分値:00の日付を20110101に上書きする場合
 *
 * システムプロパティに「BasicBusinessDateProvider.00=20110101」を設定しプロセスを起動する。
 * java -DBasicBusinessDateProvider.00=20110101 Main
 * }
 * </pre>
 *
 * @author Miki Habu
 */
public class BasicBusinessDateProvider implements BusinessDateProvider, Initializable {

    /** 業務日付テーブル物理名 */
    private String tableName;

    /** 業務日付テーブルの区分カラム物理名 */
    private String segmentColumnName;

    /** 業務日付テーブルの日付カラム物理名 */
    private String dateColumnName;

    /** 区分値省略時のデフォルト値 */
    private String defaultSegment;

    /** 取得用SQL */
    private String selectSql;

    /** 取得用SQL(全ての業務日付) */
    private String selectAllSql;

    /** 更新用SQL(業務日付) */
    private String updateDateSql;

    /** キャッシュ有無 */
    private boolean cacheEnabled = true;

    /** トランザクション名称 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /** トランザクションマネージャ */
    private SimpleDbTransactionManager transactionManager;

    /** キャッシュキー */
    private static final String CACHE_KEY = "BUSINESS_DATE";

    /**
     * クラス名(単純形式)。
     *
     * @see Class#getSimpleName()
     */
    private static final String CLASS_NAME = BasicBusinessDateProvider.class
            .getSimpleName();


    /**
     * 業務日付テーブル物理名を設定する。
     *
     * @param tableName 業務日付テーブル物理名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 業務日付テーブルの区分カラム物理名を設定する。
     *
     * @param segmentColumnName 業務日付テーブルの区分カラム物理名
     */
    public void setSegmentColumnName(String segmentColumnName) {
        this.segmentColumnName = segmentColumnName;
    }

    /**
     * 業務日付テーブルの日付カラム物理名を設定する。
     *
     * @param dateColumnName 業務日付テーブルの日付カラム物理名
     */
    public void setDateColumnName(String dateColumnName) {
        this.dateColumnName = dateColumnName;
    }

    /**
     * 区分省略時のデフォルト値を設定する。
     *
     * @param defaultSegment 区分省略時のデフォルト値
     */
    public void setDefaultSegment(String defaultSegment) {
        this.defaultSegment = defaultSegment;
    }

    /**
     * キャッシュ有無を設定する。
     * <br/>
     * 本設定を省略した場合のデフォルト動作は、キャッシュ有りとなる。
     *
     * @param cache キャッシュをするか否か。（キャッシュを行う場合は、true)
     */
    public void setCacheEnabled(boolean cache) {
        cacheEnabled = cache;
    }

    /**
     * トランザクション名称を設定する。
     * <p/>
     * 本設定は、デフォルトのトランザクション名({@link TransactionContext#DEFAULT_TRANSACTION_CONTEXT_KEY})
     * 以外のトランザクション名を使用する場合に設定を行えば良い。
     * <p/>
     * なお、本プロパティに設定したトランザクション名称に紐付く{@link AppDbConnection}が存在しない場合は、
     * {@link #setDbTransactionManager(nablarch.core.db.transaction.SimpleDbTransactionManager)}で設定された
     * トランザクションマネージャを使用して、短期的なトランザクションを用いて日付の取得処理を行う。
     *
     * @param dbTransactionName トランザクション名称。
     */
    public void setDbTransactionName(String dbTransactionName) {
        this.dbTransactionName = dbTransactionName;
    }

    /**
     * {@link SimpleDbTransactionManager トランザクションマネージャ}を設定する。
     * <p/>
     * データベースから日付を取得する際に使用するトランザクションを設定すること。
     *
     * @param transactionManager {@link SimpleDbTransactionManager トランザクションマネージャ}
     */
    public void setDbTransactionManager(
            SimpleDbTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /** {@inheritDoc}
     * リポジトリに日付が設定されている場合は、その日付を返却する。
     */
    public String getDate() {
        return getDate(defaultSegment);
    }

    /** {@inheritDoc}
     * リポジトリに日付が設定されている場合は、その日付を返却する。
     */
    public String getDate(final String segment) {

        // リポジトリに日付が設定されている場合は、
        // その日付を優先的に返却する。
        String date = getRepositoryDate(segment);
        if (date != null) {
            return date;
        }

        if (cacheEnabled) {
            // キャッシュ有りの場合は、キャッシュしたデータを返却する。
            Map<String, String> data = getCacheData();
            if (!data.containsKey(segment)) {
                throw new IllegalStateException(String.format(
                        "segment was not found. segment:%s.", segment));
            }
            return data.get(segment);
        }

        // DBコネクションの取得
        if (!DbConnectionContext.containConnection(dbTransactionName)) {
            transactionManager.setDbTransactionName(dbTransactionName);
            return new SimpleDbTransactionExecutor<String>(transactionManager) {
                @Override
                public String execute(AppDbConnection connection) {
                    return getDateBySegment(connection, segment);
                }
            }
            .doTransaction();
        } else {
            AppDbConnection con = DbConnectionContext.getConnection(
                    dbTransactionName);
            return getDateBySegment(con, segment);
        }
    }

    /**
     * 日付取得用のSQL文を実行し結果を返却する。
     *
     * @param connection {@link AppDbConnection データベース接続}
     * @param segment 区分
     * @return 取得した業務日付
     * @throws IllegalStateException 区分に対応するデータが存在しない場合
     */
    private String getDateBySegment(AppDbConnection connection,
            String segment) throws IllegalStateException {
        SqlPStatement select = connection.prepareStatement(selectSql);
        select.setString(1, segment);
        SqlResultSet resultSet = select.retrieve();
        if (resultSet.isEmpty()) {
            // 取得できない場合はエラー
            throw new IllegalStateException(String.format(
                    "segment was not found. segment:%s.", segment));
        }
        return resultSet.get(0).getString(dateColumnName);
    }

    /** {@inheritDoc} */
    public Map<String, String> getAllDate() {
        if (cacheEnabled) {
            // キャッシュがオンの場合には、キャッシュしたデータを返却する。
            return getCacheData();
        }
        return getAllBusinessDate();
    }

    /**
     * 全ての業務日付を取得する。
     * <p/>
     * 取得したデータは、下記形式の{@link Map}として返却する。
     * <pre>
     * key:区分
     * value:日付
     * </pre>
     *
     * @return 全業務日付
     */
    private Map<String, String> getAllBusinessDate() {

        SqlResultSet resultSet;
        if (!DbConnectionContext.containConnection(dbTransactionName)) {
            transactionManager.setDbTransactionName(dbTransactionName);

            resultSet = (new SimpleDbTransactionExecutor<SqlResultSet>(
                    transactionManager) {
                @Override
                public SqlResultSet execute(AppDbConnection connection) {
                    return getDateByUnCondition(connection);
                }
            }).doTransaction();
        } else {
            AppDbConnection con = DbConnectionContext.getConnection(
                    dbTransactionName);
            resultSet = getDateByUnCondition(con);
        }

        // 結果の詰め替え
        Map<String, String> ret = new HashMap<String, String>();
        for (SqlRow row : resultSet) {
            String segment = row.getString(segmentColumnName);
            String date = getRepositoryDate(segment);
            if (date == null) {
                // リポジトリに存在しない場合はDBの値を使用する。
                date = row.getString(dateColumnName);
            }
            ret.put(segment, date);
        }
        return ret;
    }

    /**
     * 条件なしで、全日付データを取得する。
     *
     * @param connection {@link AppDbConnection データベース接続}
     * @return 取得した日付データ
     * @throws IllegalStateException 日付が存在しない場合
     */
    private SqlResultSet getDateByUnCondition(
            AppDbConnection connection) throws IllegalStateException {
        SqlPStatement select = connection.prepareStatement(selectAllSql);
        SqlResultSet resultSet = select.retrieve();
        if (resultSet.isEmpty()) {
            // 取得できない場合はエラー
            throw new IllegalStateException(
                    "business date was not registered.");
        }
        return resultSet;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 指定された区分に対応する業務日付を更新する。
     *
     * @throws IllegalArgumentException 区分または、業務日付がnullまたは空文字列の場合。
     * または、指定された業務日付が'yyyyMMdd'形式でない場合。
     */
    public void setDate(final String segment,
            final String date) throws IllegalArgumentException {
        // パラメータがnullまたは空文字ならエラー
        if (segment == null) {
            throw new IllegalArgumentException("segment was null.");
        }
        if (date == null) {
            throw new IllegalArgumentException("date was null.");
        }
        if (segment.length() == 0) {
            throw new IllegalArgumentException("segment was empty.");
        }
        if (date.length() == 0) {
            throw new IllegalArgumentException("date was empty.");
        }

        // 日付フォーマットのチェック
        if (!DateUtil.isValid(date, "yyyyMMdd")) {
            throw new IllegalArgumentException(
                    "date was not formatted 'yyyyMMdd' or non existent date. date:"
                            + date + ".");
        }

        if (!DbConnectionContext.containConnection(dbTransactionName)) {
            transactionManager.setDbTransactionName(dbTransactionName);
            (new SimpleDbTransactionExecutor<Void>(transactionManager) {
                @Override
                public Void execute(AppDbConnection connection) {
                    updateDate(segment, date, connection);
                    return null;
                }
            }).doTransaction();
        } else {
            AppDbConnection con = DbConnectionContext.getConnection(
                    dbTransactionName);
            updateDate(segment, date, con);
        }
    }

    /**
     * 指定された区分に対応する日付を指定された日付に更新する。
     *
     * @param segment 区分
     * @param date 更新する日付
     * @param connection {@link AppDbConnection データベース接続}
     */
    private void updateDate(String segment, String date,
            AppDbConnection connection) {
        SqlPStatement updateDate = connection.prepareStatement(updateDateSql);
        updateDate.setString(1, date);
        updateDate.setString(2, segment);
        int updateCount = updateDate.executeUpdate();

        if (updateCount == 0) {
            throw new IllegalStateException(
                    "segment was not found. segment:" + segment + '.');
        }
    }

    /**
     * 初期化処理を行う。<br>
     * SQLを組み立てる
     */
    public void initialize() {
        // 日付取得用SQL
        String tmpSql =
                "SELECT "
                        + "$DATE_COL$ "
                        + "FROM "
                        + "$TABLE_NAME$ "
                        + "WHERE "
                        + "$SEGMENT_COL$ = ?";

        selectSql = tmpSql.replace("$DATE_COL$", dateColumnName)
                .replace("$TABLE_NAME$", tableName)
                .replace("$SEGMENT_COL$", segmentColumnName);

        // 全日付取得用SQL
        tmpSql =
                "SELECT "
                        + "$SEGMENT_COL$, "
                        + "$DATE_COL$ "
                        + "FROM "
                        + "$TABLE_NAME$ ";

        selectAllSql = tmpSql.replace("$SEGMENT_COL$", segmentColumnName)
                .replace("$DATE_COL$", dateColumnName)
                .replace("$TABLE_NAME$", tableName);

        // 日付更新用SQL
        tmpSql =
                "UPDATE "
                        + "$TABLE_NAME$ "
                        + "SET "
                        + "$DATE_COL$ = ? "
                        + "WHERE "
                        + "$SEGMENT_COL$ = ?";
        updateDateSql = tmpSql.replace("$TABLE_NAME$", tableName)
                .replace("$DATE_COL$", dateColumnName)
                .replace("$SEGMENT_COL$", segmentColumnName);
    }

    /**
     * 有効な日付の区分を保持するSet。
     * <br/>
     * リポジトリに登録された日付のバリデーションを必要最低限に留めるために、
     * バリデーション済みまたはリポジトリに未登録の区分を保持する。
     */
    private static Set<String> validSegment = new HashSet<String>();

    /**
     * リポジトリから指定された区分に対応する業務日付を取得する。
     *
     * @param segment 区分
     * @return 業務日付。リポジトリに指定された区分が存在しない場合は、null。
     */
    private static String getRepositoryDate(String segment) {
        String date = SystemRepository.getString(CLASS_NAME + '.' + segment);
        if (StringUtil.isNullOrEmpty(date)) {
            return null;
        }

        if (validSegment.contains(date)) {
            // 既にバリデーション済みの日付の場合は、バリデーションを行わずに返却する。
            return date;
        }

        if (!DateUtil.isValid(date, "yyyyMMdd")) {
            throw new IllegalStateException(
                    "business date in system repository was invalid."
                  + " date=[" + date + "] segment=[" + segment + "]");
        }
        // バリデーション済みの区分を保持
        validSegment.add(date);
        return date;
    }

    /**
     * キャッシュされた業務日付を取得する。
     * <p/>
     * キャッシュにデータが存在しない場合は、{@link #getAllBusinessDate()}を使用して、
     * 全ての業務日付を取得しキャッシュを行う。
     * <p/>
     * 本機能でキャッシュしたデータは、{@link nablarch.common.handler.threadcontext.ThreadContextHandler#handle(Object, nablarch.fw.ExecutionContext)}
     * でクリアされる。
     *
     * @return 業務日付データ
     */
    private Map<String, String> getCacheData() {
        @SuppressWarnings("unchecked")
        Map<String, String> tableData = (Map<String, String>) ThreadContext
                .getObject(CACHE_KEY);
        if (tableData != null) {
            // 既にキャッシュがされている場合はなにもしない。
            return tableData;
        }
        // 業務日付の全てのレコード情報をキャッシュする。
        Map<String, String> date = getAllBusinessDate();
        ThreadContext.setObject(CACHE_KEY, date);
        return date;
    }
}

