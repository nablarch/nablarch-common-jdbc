package nablarch.common.handler;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * テストテーブル
 */
@Entity
@Table(name = "TEST_TABLE")
public class TestTable {
    
    public TestTable() {
    };
    
    public TestTable(String col1) {
        this.col1 = col1;
    }

    @Id
    @Column(name = "COL1", length = 5, nullable = false)
    public String col1;
}
