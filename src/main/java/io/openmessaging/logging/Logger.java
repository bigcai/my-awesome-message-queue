
package io.openmessaging.logging;

/**
 * 日志输出接口
 *
 * @author jason.shang
 */
public interface Logger {

	/**
	 * 输出调试信息
	 *
	 * @param msg 信息内容
	 */
	void debug(String msg);

	/**
     * 输出调试信息
     *
     * @param e 异常信息
     */
	void debug(Throwable e);
	
	/**
	 * 输出调试信息
	 *
	 * @param msg 信息内容
	 * @param e 异常信息
	 */
	void debug(String msg, Throwable e);

	/**
	 * 输出普通信息
	 *
	 * @param msg 信息内容
	 */
	void info(String msg);

	/**
     * 输出普通信息
     *
     * @param e 异常信息
     */
	void info(Throwable e);
	
	/**
	 * 输出普通信息
	 *
	 * @param msg 信息内容
	 * @param e 异常信息
	 */
	void info(String msg, Throwable e);

	/**
	 * 输出警告信息
	 *
	 * @param msg 信息内容
	 */
	void warn(String msg);
	
	/**
     * 输出警告信息
     *
     * @param e 异常信息
     */
	void warn(Throwable e);

	/**
	 * 输出警告信息
	 *
	 * @param msg 信息内容
	 * @param e 异常信息
	 */
	void warn(String msg, Throwable e);

	/**
	 * 输出错误信息
	 *
	 * @param msg 信息内容
	 */
	void error(String msg);
	
	/**
     * 输出错误信息
     *
     * @param e 异常信息
     */
	void error(Throwable e);

	/**
	 * 输出错误信息
	 *
	 * @param msg 信息内容
	 * @param e 异常信息
	 */
	void error(String msg, Throwable e);

	/**
	 * 调试信息是否开启
	 * @param level 日志级别
	 * @return 是否开启
	 */
	boolean isEnabled(Level level);
}