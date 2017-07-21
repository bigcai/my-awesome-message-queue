package io.openmessaging.logging.jdk;

import io.openmessaging.logging.Logger;
import io.openmessaging.logging.LoggerFactory;
import org.junit.Test;

/**
 *
 * 日志测试
 *
 * @author jason.shang
 */
public class LoggingTest {

    @Test
    public void testWriteErrorLevelLog(){

        Logger logger = LoggerFactory.getLogger(LoggingTest.class);
        logger.error("test write error message.");

    }


    @Test
    public void testWriteExceptionLog(){
        Logger logger = LoggerFactory.getLogger(LoggingTest.class);
        logger.error("test write error message.", new Exception("internal error!!"));
    }
}
