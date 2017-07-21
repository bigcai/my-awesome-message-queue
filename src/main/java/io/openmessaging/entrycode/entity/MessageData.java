package io.openmessaging.entrycode.entity;

import io.openmessaging.entrycode.util.Constant;

/**
 * 标记一个MessagePage,由于读写页数据都是 先移动读写指针，在来执行真正的读写操作，因此在关闭文件时必须要求所有读写操作都停止，
 * 所以读写需要有一个“同步计数器”，当有人移动指针的时候，需要将同步计数器+1；读写操作完成后，需要将同步计数器-1；
 * 
 * 
 * ifReadOver() 判断是否读完该页文件
 * 	
 * ifWriteOver() 判断是否写完该页文件
 * 
 * Created by nbs on 2017/5/10.
 */
public class MessageData {
	
	private int pageId = 0; // 标记存放Message的文件页
    private int position = 0; // 标记该文件页的指针偏离位置
    
    private Integer readingCount = 0;
    private Integer writingCount = 0;

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

	public int getPageId() {
		return pageId;
	}

	public void setPageId(int pageId) {
		this.pageId = pageId;
	}

	// 读同步计数器
	public int getReadingCount() {
		synchronized (this.readingCount) {
			return readingCount;
		}
	}
	public void setReadingCount(int readingCount) {
		synchronized (this.readingCount) {
			this.readingCount = readingCount;
		}
	}
	// 写同步计数器
	public int getWritingCount() {
		synchronized (this.writingCount) {
			return writingCount;
		}
	}
	public void setWritingCount(int writingCount) {
		synchronized (this.writingCount) {
			this.writingCount = writingCount;
		}
	}
	
	// 判断是否读完该页文件
	synchronized Boolean ifReadOver() {
		if( readingCount == 0 ) {
			return true;
		} else {
			return false;
		}
	}
	// 判断是否写完该页文件
	synchronized Boolean ifWriteOver() {
		if( writingCount == 0 ) {
			return true;
		} else {
			return false;
		}
	}
    
}
