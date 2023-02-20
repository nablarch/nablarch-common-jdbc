package nablarch.core.message;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * テストメッセージ
 *
 */
@Entity
@Table(name = "TEST_MESSAGE")
public class TestMessage2 {

    public TestMessage2() {
    };

    public TestMessage2(String messageId, String lang,String message) {
        this.messageId = messageId;
        this.lang = lang;
        this.message = message;
    }

    @Id
    @Column(name = "MESSAGE_ID", length = 5, nullable = false)
    public String messageId;

    @Id
    @Column(name = "LANG", length = 2, nullable = false)
    public String lang;

    @Column(name = "MESSAGE", length = 200)
    public String message;
}
