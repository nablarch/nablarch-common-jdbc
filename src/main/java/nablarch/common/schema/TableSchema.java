package nablarch.common.schema;

/**
 * テーブルのスキーマ情報を保持するクラス。
 * @author Kiyohito Itoh
 */
public class TableSchema {
    
    /** テーブル名 */
    private String tableName;

    /**
     * テーブル名を設定する。
     * @param tableName テーブル名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /**
     * テーブル名を取得する。
     * @return テーブル名
     */
    public String getTableName() {
        return tableName;
    }
}
