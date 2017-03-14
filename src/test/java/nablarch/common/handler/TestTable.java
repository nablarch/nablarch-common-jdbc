package nablarch.common.handler;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

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
