package io.openmessaging.entrycode.test;

import io.openmessaging.entrycode.entity.QueueData;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.impl.MySerialization;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.ByteUtil;
import org.junit.Assert;

import java.io.Serializable;

/**
 * Created by nbs on 2017/5/29.
 */
public class Qtest implements Serializable{
    public static void main(String[] args) {
        SerializationInterface serializationInterface = new MySerialization();
        QueueIndex queueIndex = new QueueIndex();
        queueIndex.setQueueName("11111111111111");
        queueIndex.setType(1);
        queueIndex.setPageId(2);
        queueIndex.setCurrentWritePosition(4);
        queueIndex.setCurrentReadPosition(5);
        queueIndex.setStartPosition(6);
        queueIndex.setEndPosition(7);
        queueIndex.setMyPosition(8);
        byte[] bytes = serializationInterface.objectToByteArray(queueIndex);
        QueueIndex queueIndex2 = (QueueIndex)serializationInterface.byteArrayToObject(bytes);
        Assert.assertEquals(queueIndex.getCurrentReadPosition(),queueIndex2.getCurrentReadPosition());
        Assert.assertEquals(queueIndex.getCurrentWritePosition(),queueIndex2.getCurrentWritePosition());
        Assert.assertEquals(queueIndex.getPageId(),queueIndex2.getPageId());
        Assert.assertEquals(queueIndex.getStartPosition(),queueIndex2.getStartPosition());
        Assert.assertEquals(queueIndex.getEndPosition(),queueIndex2.getEndPosition());
        Assert.assertEquals(queueIndex.getType(),queueIndex2.getType());
        Assert.assertEquals(queueIndex.getMyPosition(),queueIndex2.getMyPosition());
        Assert.assertEquals(queueIndex.getQueueName(),queueIndex2.getQueueName());

        QueueData queueData = new QueueData();
        queueData.setQueueName("2222222");
        queueData.setPageId(1);
        queueData.setStartPosition(2);
        queueData.setEndPosition(3);

        byte[] bytes2 = serializationInterface.objectToByteArray(queueData);
        QueueData queueData2 = (QueueData)serializationInterface.byteArrayToObject(bytes2);
        System.out.println(queueData2);
        Assert.assertEquals(queueData2.getQueueName(),queueData.getQueueName());
        Assert.assertEquals(queueData2.getPageId(),queueData.getPageId());
        Assert.assertEquals(queueData2.getStartPosition(),queueData.getStartPosition());
        Assert.assertEquals(queueData2.getEndPosition(),queueData.getEndPosition());

    }
}