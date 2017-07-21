package io.openmessaging.entrycode.util;

import io.openmessaging.entrycode.entity.QueueIndex;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by linl on 2017/6/2.
 */
public class MappedUtil {
    public static void checkAndUmMapQueueData(ConcurrentHashMap<String, List<QueueIndex>> queueIndexsMap, ConcurrentHashMap<Integer, MappedByteBuffer> queueDataBuffer) {
        Set<Integer> unUsePageIdSet = new HashSet<Integer>(queueDataBuffer.keySet());
        List<QueueIndex> temp;
        for (Map.Entry<String, List<QueueIndex>> m : queueIndexsMap.entrySet()) {
            temp = m.getValue();
            for (QueueIndex queueIndex : temp) {
                if (queueIndex.getCurrentWritePosition() < queueIndex.getEndPosition()) {
                    unUsePageIdSet.remove(queueIndex.getPageId());
                }
            }
        }
        if (unUsePageIdSet != null && unUsePageIdSet.size() > 0) {
            for (int i : unUsePageIdSet) {
                MappedByteBuffer mappedByteBuffer = queueDataBuffer.get(i);
                if (mappedByteBuffer != null) {
                    Cleaner cl = ((DirectBuffer) mappedByteBuffer).cleaner();
                    if (cl != null) {
                        cl.clean();
                    }
                    queueDataBuffer.remove(i);
                    mappedByteBuffer = null;
                }
            }
        }
    }

    // 线程安全
    public static void umMapMessageData(int currentMessageDataPageId, ConcurrentHashMap<Integer, MappedByteBuffer> messageDataBuffer) {
    	MappedByteBuffer mappedByteBuffer = messageDataBuffer.get(currentMessageDataPageId);
    	if (mappedByteBuffer != null) {
           Cleaner cl = ((DirectBuffer) mappedByteBuffer).cleaner();
           if (cl != null) {
                    cl.clean();
          }
          messageDataBuffer.remove(currentMessageDataPageId);
          mappedByteBuffer = null;
        }
    }

    public static void checkAndUmMapEndReadQueueData(ConcurrentHashMap<String, List<QueueIndex>> queueIndexsMap, ConcurrentHashMap<Integer, MappedByteBuffer> queueDataBuffer) {
        Set<Integer> unUsePageIdSet = new HashSet<Integer>(queueDataBuffer.keySet());
        List<QueueIndex> temp;
        for (Map.Entry<String, List<QueueIndex>> m : queueIndexsMap.entrySet()) {
            temp = m.getValue();
            for (QueueIndex queueIndex : temp) {
                if (queueIndex.getCurrentReadPosition() < queueIndex.getEndPosition()) {
                    unUsePageIdSet.remove(queueIndex.getPageId());
                }
            }
        }
        if (unUsePageIdSet != null && unUsePageIdSet.size() > 0) {
            for (int i : unUsePageIdSet) {
                MappedByteBuffer mappedByteBuffer = queueDataBuffer.get(i);
                if (mappedByteBuffer != null) {
                    Cleaner cl = ((DirectBuffer) mappedByteBuffer).cleaner();
                    if (cl != null) {
                        cl.clean();
                    }
                    queueDataBuffer.remove(i);
                    mappedByteBuffer = null;
                }
            }
        }
    }

    public static void checkAndUmMapEndReadMessageData(ConcurrentHashMap<Integer, MappedByteBuffer> messageDataBuffer) {
        //Set<Integer> unUsePageIdSet = new HashSet<Integer>(messageDataBuffer.keySet());
        List<Integer> list = new ArrayList(messageDataBuffer.keySet());
        Collections.sort(list);
        int pageId = list.get(0);
        MappedByteBuffer mappedByteBuffer = messageDataBuffer.get(pageId);
        if (mappedByteBuffer != null) {
            Cleaner cl = ((DirectBuffer) mappedByteBuffer).cleaner();
            if (cl != null) {
                cl.clean();
            }
            messageDataBuffer.remove(pageId);
            mappedByteBuffer = null;
        }
    }

    public static void unMapQueueDataByFileNum(ConcurrentHashMap<Integer, MappedByteBuffer> queueDataBuffer){
        unMapByFileNum(queueDataBuffer);
    }

    public static void unMapMessageDataByFileNum(ConcurrentHashMap<Integer, MappedByteBuffer> messageDataBuffer){
        unMapByFileNum(messageDataBuffer);
    }

    private static void unMapByFileNum(ConcurrentHashMap<Integer, MappedByteBuffer> DataBuffer){
        List<Integer> list = new ArrayList(DataBuffer.keySet());
        if(list != null && list.size() > Constant.MAX_MAPPED_FILE_NUM){
            Collections.sort(list);
            int pageId =list.get(0);
            MappedByteBuffer mappedByteBuffer = DataBuffer.get(pageId);
            mappedByteBuffer.force();
            DataBuffer.remove(pageId);
            if (mappedByteBuffer != null) {
                Cleaner cl = ((DirectBuffer) mappedByteBuffer).cleaner();
                if (cl != null) {
                    cl.clean();
                }
                mappedByteBuffer = null;
            }
        }

    }




}
