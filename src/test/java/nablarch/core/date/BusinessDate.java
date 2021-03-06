package nablarch.core.date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * テスト用の業務日付テーブル
 *
 */
@Entity
@Table(name = "BUSINESS_DATE")
public class BusinessDate {

    public BusinessDate() {
    };

    public BusinessDate(String segment, String bizDate) {
        this.segment = segment;
        this.bizDate = bizDate;
    }

    @Id
    @Column(name = "SEGMENT", length = 2, nullable = false)
    public String segment;

    @Column(name = "BIZ_DATE", length = 8, nullable = false)
    public String bizDate;
}
