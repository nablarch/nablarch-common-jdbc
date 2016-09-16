package nablarch.core.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.cache.StaticDataLoader;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;

/**
 * StringResourceHolderが使うキャッシュに必要な文字列リソースをデータベースから取得するクラス。<br/>
 * StringResourceの実装にはBasicStringResourceを用いる。
 * 
 * 
 * @author Koichi Asano
 *
 */
public class BasicStringResourceLoader implements StaticDataLoader<StringResource> {
    /**
     * データロードに使用するSimpleDbTransactionManagerのインスタンス。
     */
    private SimpleDbTransactionManager dbManager;
    /**
     * メッセージが格納されたテーブルのテーブル名。
     */
    private String tableName;
    /**
     * メッセージIDカラム名。
     */
    private String idColumnName;
    /**
     * 言語カラム名。
     */
    private String langColumnName;
    /**
     * メッセージカラム名。
     */
    private String valueColumnName;
    
    /**
     * SQL文作成済みフラグ。
     */
    private boolean idQueryCreated = false;
    
    /**
     * IDによるクエリ用SQL文
     */
    private String selectByIdQuery = null;

    /**
     * データロードに使用するDbManagerのインスタンスをセットする。
     * @param dbManager データロードに使用するDbManagerのインスタンス
     */
    public void setDbManager(SimpleDbTransactionManager dbManager) {
        this.dbManager = dbManager;
    }

    /**
     * メッセージが格納されたテーブルのテーブル名をセットする。
     * @param tableName メッセージが格納されたテーブルのテーブル名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
        idQueryCreated = false;
    }

    /**
     * メッセージIDカラム名をセットする。
     * @param idColumnName メッセージIDカラム名
     */
    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
        idQueryCreated = false;
    }

    /**
     * 言語カラム名をセットする。
     * @param langColumnName 言語カラム名
     */
    public void setLangColumnName(String langColumnName) {
        this.langColumnName = langColumnName;
        idQueryCreated = false;
    }

    /**
     * メッセージカラム名をセットする。
     * @param valueColumnName メッセージカラム名
     */
    public void setValueColumnName(String valueColumnName) {
        this.valueColumnName = valueColumnName;
        idQueryCreated = false;
    }

    /**
     * {@inheritDoc}
     */
    public Object getId(StringResource value) {
        return value.getId();
    }

    /**
     * {@inheritDoc}<br/>
     * 本実装ではindexの使用を想定しないため、nullを返す。
     */
    public Object generateIndexKey(String indexName, StringResource value) {
        // indexは提供しない
        return null;
    }


    /**
     * {@inheritDoc}<br/>
     * 本実装ではindexの使用を想定しないため、nullを返す。
     */
    public List<String> getIndexNames() {
        // indexは提供しない
        return null;
    }

    /**
     * メッセージを格納したテーブルからメッセージIDに対応するメッセージを取得する。
     * 
     * @param id メッセージID
     * @return メッセージIDに対応するメッセージ
     * 
     */
    public StringResource getValue(final Object id) {
        boolean requireTransaction = !DbConnectionContext.containConnection(dbManager.getDbTransactionName());
        SqlResultSet resultSet;
        if (requireTransaction) {
            resultSet = new SimpleDbTransactionExecutor<SqlResultSet>(dbManager) {
                @Override
                public SqlResultSet execute(AppDbConnection connection) {
                    return getMessage(id);
                }
            }
            .doTransaction();
        } else {
            resultSet = getMessage(id);
        }
        List<StringResource> messages = createMessage(resultSet);
        return messages.isEmpty() ? null : messages.get(0);
    }

    /**
     * 指定されたIDに紐づくメッセージ情報をデータベースから取得する。
     * @param id ID
     * @return 取得したメッセージ情報
     */
    private SqlResultSet getMessage(Object id) {
        AppDbConnection connection = DbConnectionContext.getConnection(
                dbManager.getDbTransactionName());
        if (!idQueryCreated) {
            // SQL文が作られていなければ、作成
            selectByIdQuery = "SELECT "
                    + idColumnName + ", "
                    + langColumnName + ", "
                    + valueColumnName + " "
                    + " FROM " + tableName + " "
                    + " WHERE " + idColumnName + " = ? "
                    + " ORDER BY " + idColumnName + ", " + langColumnName;
            idQueryCreated = true;
        }
        SqlPStatement prepared = connection.prepareStatement(selectByIdQuery);
        prepared.setString(1, id.toString());
        return prepared.retrieve();
    }

    /**
     * メッセージを格納したテーブルからメッセージIDに対応するメッセージを取得する。
     * 
     * @param indexName インデックス名
     * @param key 静的データのキー
     * @return インデックス名、キーに対応するデータのリスト
     * 
     */
    public List<StringResource> getValues(String indexName, Object key) {
        // indexは提供しない
        return null;
    }

    /**
     * メッセージを格納したテーブルから全てのメッセージを取得する。
     * 
     * @return 全てのメッセージのリスト。
     */
    public List<StringResource> loadAll() {
        SqlResultSet resultSet = new SimpleDbTransactionExecutor<SqlResultSet>(
                dbManager) {
            @Override
            public SqlResultSet execute(AppDbConnection connection) {
                String selectAllQuery = "SELECT "
                        + idColumnName + ", "
                        + langColumnName + ", "
                        + valueColumnName + " "
                        + " FROM " + tableName + " "
                        + " ORDER BY " + idColumnName + ", " + langColumnName;
                SqlPStatement prepared = connection.prepareStatement(
                        selectAllQuery);
                return prepared.retrieve();
            }
        }
        .doTransaction();
        return createMessage(resultSet);
    }

    /**
     * SqlResultSetを元に、メッセージのリストを作成する。
     * 
     * @param results 元となるSqlResultSet
     * @return 作成したメッセージのリスト
     */
    private List<StringResource> createMessage(SqlResultSet results) {
        List<StringResource> msgs = new ArrayList<StringResource>(results.size());
        if (results.isEmpty()) {
            return msgs;
        }
        
        Map<String, String> formatMap = null;
        String msgId = null;
        for (SqlRow row : results) {
            String currentMsgId = row.getString(idColumnName);
            String lang = row.getString(langColumnName);
            String value = row.getString(valueColumnName);
            
            if (!currentMsgId.equals(msgId)) {
                String prevMsgId = msgId;
                msgId = currentMsgId;
                if (prevMsgId != null) {
                    BasicStringResource msg = new BasicStringResource(prevMsgId, formatMap);
                    msgs.add(msg);
                }
                formatMap = new HashMap<String, String>();
            }
            formatMap.put(lang, value);
        }
        BasicStringResource msg = new BasicStringResource(msgId, formatMap);
        msgs.add(msg);
        
        return msgs;
    }
}

