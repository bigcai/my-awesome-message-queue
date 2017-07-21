package io.openmessaging.entrycode.impl;

import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.interfaces.FileDataLoader;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.Constant;
import io.openmessaging.entrycode.util.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by nbs on 2017/5/6.
 */
public class NIOFileLoader implements FileDataLoader {
    private String defaultpath;
    private File[] files;

    NIOFileLoader(String path) {
        this.defaultpath = path;
        files = FileUtil.getFilesByPath(defaultpath);
    }

    private List<File> getQueueIndexFiles() {
        return getFilesByType(Constant.DEFAULT_QUEUE_INDEX_NAME);
    }

    private List<File> getQueueDataFiles() {
        return getFilesByType(Constant.DEFAULT_QUEUE_DATA_NAME);
    }

    private List<File> getMessageDataFiles() {
        return getFilesByType(Constant.DEFAULT_MESSAGE_DATA_NAME);
    }

    private List<File> getFilesByType(String type) {
        List<File> fileList = new ArrayList<File>();
        for (File file : files) {
            if (file.getName().contains(type)) {
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                fileList.add(file);
            }
        }
        return fileList;
    }

    private ConcurrentHashMap<Integer, MappedByteBuffer> loadFilesIntoBuffer(List<File> list, int maxFileNum) throws IOException {
        ConcurrentHashMap<Integer, MappedByteBuffer> queueIndexes = new ConcurrentHashMap<Integer, MappedByteBuffer>();
        String fileName;
        maxFileNum = maxFileNum < list.size() ? maxFileNum : list.size();
        for (int i = 0; i < maxFileNum; i++) {
            fileName = list.get(i).getName();
            int pageId = FileUtil.getIdByName(fileName);
            FileChannel fileChannel = FileChannel.open(Paths.get(defaultpath + list.get(i).getName()), StandardOpenOption.READ, StandardOpenOption.WRITE);
            long maxFileLength = getMaxFileSize(fileName, fileChannel.size());
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileLength);
            queueIndexes.putIfAbsent(pageId, mappedByteBuffer);
        }
        return queueIndexes;
    }

    public MappedByteBuffer mappedFileByName(String fileName) throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(defaultpath + fileName), StandardOpenOption.READ, StandardOpenOption.WRITE);
        long maxFileLength = getMaxFileSize(fileName, fileChannel.size());
        return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileLength);
    }

    private long getMaxFileSize(String fileName, long channelSize) {
        long maxFileSize;
        if (fileName.contains(Constant.DEFAULT_QUEUE_INDEX_NAME)) {
            maxFileSize = Constant.MAX_QUEUE_INDEX_FILE_SIZE;
        } else if (fileName.contains(Constant.DEFAULT_QUEUE_DATA_NAME)) {
            maxFileSize = Constant.MAX_QUEUE_DATA_FILE_SIZE;
        } else {
            maxFileSize = Constant.MAX_MESSAGE_DATA_FILE_SIZE;
        }
        return channelSize >= maxFileSize ? channelSize : maxFileSize;
    }

    @Override
    public MappedByteBuffer loadQueueIndexFiles() throws IOException {
        return loadFilesIntoBuffer(getQueueIndexFiles(), Constant.MAX_MAPPED_FILE_NUM).get(1);
    }

    @Override
    public ConcurrentHashMap<Integer, MappedByteBuffer> loadQueueDataFiles() throws IOException {
        return loadFilesIntoBuffer(getQueueDataFiles(), Constant.MAX_MAPPED_FILE_NUM);
    }

    @Override
    public ConcurrentHashMap<Integer, MappedByteBuffer> loadMessageDataFiles() throws IOException {
        return loadFilesIntoBuffer(getMessageDataFiles(), Constant.MAX_MAPPED_FILE_NUM);
    }

    @Override
    public ConcurrentHashMap<String, List<QueueIndex>> loadQueueIndex(MappedByteBuffer queueIndexBuffer) throws IOException {
    	ConcurrentHashMap<String, List<QueueIndex>> map = new ConcurrentHashMap<>();
        for (int i = 0; i < Constant.MAX_QUEUE_INDEX_FILE_SIZE; i = i + Constant.QUEUE_INDEX_LENGTH) {
            byte[] dest = new byte[Constant.QUEUE_INDEX_LENGTH];
            queueIndexBuffer.get(dest);//顺序读
            if(new String(dest).trim().length() == 0)continue;
            QueueIndex queueIndex = (QueueIndex)NIOMessageStore.serializationInterface.byteArrayToObject(dest);
            if (queueIndex.getQueueName() != null && queueIndex.getQueueName().length() != 0 && queueIndex.getCurrentReadPosition() != queueIndex.getEndPosition()) {
                List<QueueIndex> queueIndexList = map.get(queueIndex.getQueueName());
                if(queueIndexList == null){
                    queueIndexList = new ArrayList<QueueIndex>();
                }
                queueIndexList.add(queueIndex);
                map.put(queueIndex.getQueueName(), queueIndexList);
            }
        }
        return map;
    }

    public long getQueueIndexFileSize() {
        List<File> fileList = getFilesByType(Constant.DEFAULT_QUEUE_INDEX_NAME);
        if (fileList != null && fileList.size() != 0) {
            File file = fileList.get(0);
            return file.length();
        }
        return 0L;
    }
}
