package io.openmessaging.entrycode.util;

/**
 * Created by nbs on 2017/5/6.
 */
public class Constant {

    public final static String DEFAULT_QUEUE_INDEX_NAME="queue_index";
    public final static String DEFAULT_QUEUE_DATA_NAME="queue_Data";
    public final static String DEFAULT_MESSAGE_DATA_NAME="message_data";

    public final static String STORE_PATH="D:/awesome-message/";
    public final static String FILE_FORMATE_PATTERN = "^[0-999]{1,9}-.{1,}$";

    public final static long MAX_QUEUE_INDEX_FILE_SIZE = Constant.QUEUE_INDEX_LENGTH*300000;
    public final static int MAX_QUEUE_DATA_FILE_SIZE = Constant.QUEUE_DATA_LENGTH * Constant.QUEUE_DATA_NUM_IN_INDEX * 220  ;
    public final static long MAX_MESSAGE_DATA_FILE_SIZE = 1000*1000*800;//1000*1000*800

    /**姣忎釜queueData鐨勯暱搴�*/
    public final static int QUEUE_DATA_LENGTH = 50;

    /**每个queueIndex的长度**/
    public final static int QUEUE_INDEX_LENGTH = 70;

    /**涓�釜queueIndex绱㈠紩queueData鐨勪釜鏁�*/
    public final static int QUEUE_DATA_NUM_IN_INDEX = 300000;//30000

    /**文件扩容比例*/
    public final static double QUEUE_DATA_RATE = 0.7;
    public final static double MESSAGE_DATA_RATE = 0.7;


    /*最大映射文件数*/
    public static final int MAX_MAPPED_FILE_NUM = 99999;//3;
    
    public static final int TOPIC_TYPE = 0;
    public static final int QUEUE_TYPE = 1;
	public static final int OFFICAL_TEST_MAX_MSG_LENGTH = 300 * 2014; // 官方最大消息为256KB
}
