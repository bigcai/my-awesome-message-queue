package io.openmessaging.entrycode.entity;

import io.openmessaging.entrycode.impl.JavaSerializationImpl;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.Constant;

import java.io.Serializable;

/**
 * 多个QueueIndex组成队列，一个QueueIndex由N固定长度的queueData组成
 * 
 * Created by nbs on 2017/5/6.
 */
public class QueueIndex  implements Serializable  {
    
    //private byte[] queueName = new byte[99];//队列名称
    private String queueName = "";//队列名称
    private int type = -1;//类型 0 topic ，1 queue

    private int currentReadPosition = 0;
    private int currentWritePosition = 0;

    private int PageId = 0;//标记QueueIndex对应哪个页文件

    private int myPosition;//本queueindex在内存中的地址(写时固定下来)
    private int startPosition;// startPosition紧接上一个被初始化的 QueueIndex 的endPosition。(被初始化时固定下来)
    private int endPosition;//endPosition - startPosition = QueueIndex的阀值(被初始化时固定下来)
    
    @Override
    public String toString() {
        return "QueueIndex{" +
                "queueName='" + queueName + '\'' +
                ", type=" + type +
                ", startPosition=" + startPosition +
                ", endPosition=" + endPosition +
                ", currentReadPosition=" + currentReadPosition +
                ", currentWritePosition=" + currentWritePosition +
                ", PageId=" + PageId +
                ", myPosition=" + myPosition +
                '}';
    }

    public String getQueueName() {
        //return new String(queueName).trim();
        return queueName;
    }

    public void setQueueName(String queueName) {
       /* byte[] bytes = queueName.getBytes();
        System.arraycopy(bytes,0,this.queueName,0,bytes.length);*/
       this.queueName = queueName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCurrentReadPosition() {
        return currentReadPosition;
    }

    public void setCurrentReadPosition(int currentReadPosition) {
        this.currentReadPosition = currentReadPosition;
    }

    public int getCurrentWritePosition() {
        return currentWritePosition;
    }

    public void setCurrentWritePosition(int currentWritePosition) {
        this.currentWritePosition = currentWritePosition;
    }

    public int getPageId() {
        return PageId;
    }

    public void setPageId(int pageId) {
        PageId = pageId;
    }

    public int getMyPosition() {
        return myPosition;
    }

    public void setMyPosition(int myPosition) {
        this.myPosition = myPosition;
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

	public boolean isFull() {
		if( currentWritePosition + Constant.QUEUE_DATA_LENGTH > endPosition) {
			return true;
		} else {
			return false;
		}
	}

}
