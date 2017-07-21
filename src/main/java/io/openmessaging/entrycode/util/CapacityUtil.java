package io.openmessaging.entrycode.util;

import io.openmessaging.entrycode.entity.CurrentPage;
import io.openmessaging.entrycode.interfaces.FileDataLoader;
import io.openmessaging.entrycode.interfaces.FileInit;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by nbs on 2017/6/3.
 */
public class CapacityUtil {

    public static void capacityMessageData(CurrentPage currentMessageDataPage, int messageLength, ConcurrentHashMap messageDataBuffer
            , FileInit fileInit, FileDataLoader fileDataLoader) throws IOException {
        if (currentMessageDataPage.getOffset() + messageLength > Constant.MAX_MESSAGE_DATA_FILE_SIZE) {
            //messageDataPage写满了，则重新创建新页
            int messageDataPageId = fileInit.initMessageDataFile();
            currentMessageDataPage.setPageId(messageDataPageId);
            currentMessageDataPage.setOffset(0);
            MappedByteBuffer mappedByteBuffer = fileDataLoader.mappedFileByName(messageDataPageId + "-" + Constant.DEFAULT_MESSAGE_DATA_NAME);
            messageDataBuffer.putIfAbsent(messageDataPageId, mappedByteBuffer);
        }
    }

    public static void capacityMessageDataByRate(CurrentPage currentMessageDataPage, ConcurrentHashMap messageDataBuffer
            , FileInit fileInit, FileDataLoader fileDataLoader) throws IOException {
        if (currentMessageDataPage.getOffset() / Constant.MAX_MESSAGE_DATA_FILE_SIZE > Constant.MESSAGE_DATA_RATE) {
            //messageDataPage写满了，则重新创建新页
            int messageDataPageId = fileInit.initMessageDataFile();
            messageDataBuffer.putIfAbsent(messageDataPageId, fileDataLoader.mappedFileByName(messageDataPageId + "-" + Constant.DEFAULT_MESSAGE_DATA_NAME));
            synchronized (currentMessageDataPage){
                currentMessageDataPage.setPageId(messageDataPageId);
                currentMessageDataPage.setOffset(0);
            }
        }
    }

    public static void capacityQueueDataByRate(CurrentPage currentQueueDataPage, ConcurrentHashMap queueDataBuffer,
                                              FileInit fileInit, FileDataLoader fileDataLoader) throws IOException {
        if (currentQueueDataPage.getOffset() / Constant.MAX_QUEUE_DATA_FILE_SIZE > Constant.QUEUE_DATA_RATE) {
            int pageid = fileInit.initQueueDataFile();
            queueDataBuffer.putIfAbsent(pageid, fileDataLoader.mappedFileByName(pageid + "-" + Constant.DEFAULT_QUEUE_DATA_NAME));
            synchronized (currentQueueDataPage){
                currentQueueDataPage.setOffset(0);
                currentQueueDataPage.setPageId(pageid);
            }
        }
    }

    public static int capacityQueueData(CurrentPage currentQueueDataPage, ConcurrentHashMap queueDataBuffer,
                                        FileInit fileInit, FileDataLoader fileDataLoader) throws IOException {
        if (currentQueueDataPage.getOffset() + Constant.QUEUE_DATA_LENGTH * Constant.QUEUE_DATA_NUM_IN_INDEX > Constant.MAX_QUEUE_DATA_FILE_SIZE) {
            int pageid = fileInit.initQueueDataFile();
            currentQueueDataPage.setOffset(0);
            currentQueueDataPage.setPageId(pageid);
            queueDataBuffer.putIfAbsent(pageid, fileDataLoader.mappedFileByName(pageid + "-" + Constant.DEFAULT_QUEUE_DATA_NAME));
            return pageid;
        }
        return -1;
    }

}
