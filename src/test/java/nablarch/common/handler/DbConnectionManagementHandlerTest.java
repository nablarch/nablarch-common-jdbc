package nablarch.common.handler;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DbConnectionManagementHandlerTest {

    private DbConnectionManagementHandler target = new DbConnectionManagementHandler();

    private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class, RETURNS_DEEP_STUBS);

    private final ExecutionContext context = mock(ExecutionContext.class);

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
            when(context.handleNext(null)).thenReturn(new Result.Success());
            
            assertTrue(((Result) target.handle(null, context)).isSuccess());
    
            verify(connectionFactory).getConnection("connection");
        }


        {
            reset(connectionFactory);
            reset(context);
            
            // コンテキストにすでにコネクションが登録されていた場合
            when(context.handleNext(null)).thenReturn(new Result.Success());
            
            DbConnectionContext.setConnection("connection", connectionFactory.getConnection("connection"));
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (IllegalStateException e) {
                
            }
            
            verify(connectionFactory).getConnection("connection");
    
            DbConnectionContext.removeConnection("connection");

        }        

        {
            reset(connectionFactory);
            reset(context);
            
            // 後続のハンドラで RuntimeException
            when(context.handleNext(null)).thenThrow(new RuntimeException("test"));
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (RuntimeException e) {
                
            }
            
            verify(connectionFactory).getConnection("connection");
        }

        {
            reset(connectionFactory);
            reset(context);

            // 後続のハンドラで Error
            when(context.handleNext(null)).thenThrow(new Error("test"));
            
            try {
                assertTrue(((Result) target.handle(null, context)).isSuccess());
                fail("例外が発生するはず");
            } catch (Error e) {
                
            }
            
            verify(connectionFactory).getConnection("connection");
        }        

    }

    @Test
    public void testHandleInbound() {
        
        assertTrue(target.handleInbound(context).isSuccess());

        verify(connectionFactory).getConnection("connection");
    }

    @Test
    public void testHandleOutbound() {

        target.handleInbound(context);
        assertTrue(target.handleOutbound(context).isSuccess());

    }

}
