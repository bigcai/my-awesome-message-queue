package io.openmessaging.entrycode.entity;

import io.openmessaging.entrycode.impl.JavaSerializationImpl;
import io.openmessaging.entrycode.interfaces.SerializationInterface;

import java.io.Serializable;

/**
 * Created by nbs on 2017/5/6.
 */
public class QueueData implements Serializable {


    public String queueName ;//= new byte[99];//队列唯一ID，QueueData和queueIndex是多对1关系，通过这个ID知道该Data的归属
    public int pageId;//标记Message存在哪个页中
    public int startPosition;//NIO中起始位置
    public int endPosition;//NIO中结束位置


    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public int getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(int startPosition) {
        this.startPosition = startPosition;
    }

    public int getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(int endPosition) {
        this.endPosition = endPosition;
    }

    public String getQueueName() {
        //return new String(queueName).trim();
        return queueName;
    }

    public void setQueueName(String queueName) {
        /*byte[] bytes = queueName.getBytes();
        System.arraycopy(bytes,0,this.queueName,0,bytes.length);*/
        this.queueName = queueName;
    }

    @Override
    public String toString() {
        return "QueueData{" +
                "queueName='" + queueName + '\'' +
                ", pageId=" + pageId +
                ", startPosition=" + startPosition +
                ", endPosition=" + endPosition +
                '}';
    }
}
