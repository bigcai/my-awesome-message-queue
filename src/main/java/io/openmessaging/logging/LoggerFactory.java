
package io.openmessaging.logging;

import io.openmessaging.logging.jdk.JdkLoggerAdapter;
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 日志输出器工厂
 * 
 * @author jason.shang
 */
public class LoggerFactory {

	private LoggerFactory() {
	}

	private static volatile LoggerAdapter loggerAdapter;
	
	private static final ConcurrentMap<String, Logger> LOGGERS = new ConcurrentHashMap<String, Logger>();

	// 适配开源日志框架，如果无法找到则使用jdk日志框架
	static {
	    String logger = System.getProperty("messaging.logger");
	    if ("log4j".equals(logger)) {
            // 适配log4j
            throw new UnsupportedOperationException("log4j not be implemented yet.");
    	} else {
    		try {
                // 适配jdk
                setLoggerAdapter(new JdkLoggerAdapter());
            } catch (Throwable e) {
                System.err.println("Failed to initialize logger adapter, cause: " + e.getMessage());
			}
    	}
	}

	/**
	 * 设置日志输出器供给器
	 * 
	 * @param loggerAdapter
	 *            日志输出器供给器
	 */
	public static void setLoggerAdapter(LoggerAdapter loggerAdapter) {
		if (loggerAdapter != null) {
			Logger logger = loggerAdapter.getLogger(LoggerFactory.class.getName());
			logger.info("using logger: " + loggerAdapter.getClass().getName());
			LoggerFactory.loggerAdapter = loggerAdapter;
		}
	}

	/**
	 * 获取日志输出器
	 * 
	 * @param key
	 *            分类键
	 * @return 日志输出器
	 */
	public static Logger getLogger(Class<?> key) {
        String logger = key.getName();
        return getLogger(logger);
	}

	/**
	 * 获取日志输出器
	 * 
	 * @param key
	 *            分类键
	 * @return 日志输出器, 后验条件: 不返回null.
	 */
	public static Logger getLogger(String key) {
		Logger logger = LOGGERS.get(key);
		if (logger == null) {
			LOGGERS.putIfAbsent(key, loggerAdapter.getLogger(key));
			logger = LOGGERS.get(key);
		}
		return logger;
	}
	
	/**
	 * 动态设置输出日志级别
	 * 
	 * @param level 日志级别
	 */
	public static void setLevel(Level level) {
		loggerAdapter.setLevel(level);
	}

	/**
	 * 获取日志级别
	 * 
	 * @return 日志级别
	 */
	public static Level getLevel() {
		return loggerAdapter.getLevel();
	}
	
	/**
	 * 获取日志文件
	 * 
	 * @return 日志文件
	 */
	public static File getFile() {
		return loggerAdapter.getFile();
	}

}