package io.openmessaging.entrycode.entity;

public class CurrentQueueDataPage {

	private int pageId;
	private int offset;
	
	public CurrentQueueDataPage(int pageId, int offset) {
		super();
		this.pageId = pageId;
		this.offset = offset;
	}
	public int getPageId() {
		return pageId;
	}
	public void setPageId(int pageId) {
		this.pageId = pageId;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	

}
