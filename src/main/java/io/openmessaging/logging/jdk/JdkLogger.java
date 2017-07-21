
package io.openmessaging.logging.jdk;

import io.openmessaging.logging.Logger;
import java.util.logging.Level;

public class JdkLogger implements Logger {

	private final java.util.logging.Logger logger;

	public JdkLogger(java.util.logging.Logger logger) {
		this.logger = logger;
	}

	public void debug(String msg) {
		logger.log(Level.FINE, msg);
	}

    public void debug(Throwable e) {
        logger.log(Level.FINE, e.getMessage(), e);
    }

	public void debug(String msg, Throwable e) {
		logger.log(Level.FINE, msg, e);
	}

	public void info(String msg) {
		logger.log(Level.INFO, msg);
	}

	public void info(String msg, Throwable e) {
		logger.log(Level.INFO, msg, e);
	}

	public void warn(String msg) {
		logger.log(Level.WARNING, msg);
	}

	public void warn(String msg, Throwable e) {
		logger.log(Level.WARNING, msg, e);
	}

	public void error(String msg) {
		logger.log(Level.SEVERE, msg);
	}

	public void error(String msg, Throwable e) {
		logger.log(Level.SEVERE, msg, e);
	}

	public void error(Throwable e) {
        logger.log(Level.SEVERE, e.getMessage(), e);
    }

    public void info(Throwable e) {
        logger.log(Level.INFO, e.getMessage(), e);
    }

    public void warn(Throwable e) {
        logger.log(Level.WARNING, e.getMessage(), e);
    }

    @Override
    public boolean isEnabled(io.openmessaging.logging.Level level) {
        java.util.logging.Level currentLevel = JdkLoggerAdapter.toJdkLevel(level);
        java.util.logging.Level jdkLevel = this.logger.getLevel();
        return currentLevel.intValue() >= jdkLevel.intValue();
    }
}