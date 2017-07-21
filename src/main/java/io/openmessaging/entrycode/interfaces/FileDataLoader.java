package io.openmessaging.entrycode.interfaces;

import io.openmessaging.entrycode.entity.QueueIndex;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by nbs on 2017/5/6.
 */
public interface FileDataLoader {
    MappedByteBuffer loadQueueIndexFiles() throws IOException ;

    ConcurrentHashMap<Integer, MappedByteBuffer> loadQueueDataFiles() throws IOException ;

    ConcurrentHashMap<Integer, MappedByteBuffer> loadMessageDataFiles() throws IOException ;

    ConcurrentHashMap<String,List<QueueIndex>> loadQueueIndex(MappedByteBuffer queueIndexBuffer) throws IOException;

    long getQueueIndexFileSize();

    MappedByteBuffer mappedFileByName(String fileName) throws IOException;
    
}
