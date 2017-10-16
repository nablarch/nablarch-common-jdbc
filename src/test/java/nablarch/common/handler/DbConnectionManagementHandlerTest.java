package nablarch.common.handler;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DbConnectionManagementHandlerTest {

    private DbConnectionManagementHandler target = new DbConnectionManagementHandler();

    @Mocked
    private ConnectionFactory connectionFactory;

    @Mocked
    private ExecutionContext context = new ExecutionContext();

    @Before
    public void setup() {
        DbConnectionContext.removeConnection("connection");
        target.setConnectionFactory(connectionFactory);
        target.setConnectionName("connection");
    }

    @Test
    public void testHandle() {
        {
            // 正常系
            new Expectations() {{
                context.handleNext(null);
                result = new Result.Success();
            }};        
            
            assertTrue(((Result) target.handle(null, context)).isSuccess());
    
            new Verifications() {{
                connectionFactory.getConnection("connection");
                times = 1;
            }};
        }


        {
            // コンテキストにすでにコネクションが登録されていた場合
            new Expectations() {{
                context.handleNext(null);
                result = new Result.Success();
            }};        
            
            DbConnectionContext.setConnection("connection", connectionFactory.getConnection("connection"));
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (IllegalStateException e) {
                
            }
            new Verifications() {{
                connectionFactory.getConnection("connection");
                times = 1;
            }};
    
            DbConnectionContext.removeConnection("connection");

        }        

        {
            // 後続のハンドラで RuntimeException
            new Expectations() {{
                context.handleNext(null);
                result = new RuntimeException("test");
            }};        
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (RuntimeException e) {
                
            }
            new Verifications() {{
                connectionFactory.getConnection("connection");
                times = 1;
            }};
        }

        {
            // 後続のハンドラで Error
            new Expectations() {{
                context.handleNext(null);
                result = new Error("test");
            }};        
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (Error e) {
                
            }
            new Verifications() {{
                connectionFactory.getConnection("connection");
                times = 1;
            }};
        }        

    }

    @Test
    public void testHandleInbound() {
        
        assertTrue(target.handleInbound(context).isSuccess());

        new Verifications() {{
            connectionFactory.getConnection("connection");
            times = 1;
        }};
    }

    @Test
    public void testHandleOutbound() {

        target.handleInbound(context);
        assertTrue(target.handleOutbound(context).isSuccess());

    }

}
