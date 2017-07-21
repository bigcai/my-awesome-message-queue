package io.openmessaging.entrycode.entity;


import java.util.ArrayList;
import java.util.List;

/**
 * User: vk
 * Date: 2017/4/28
 * Time: 12:01
 * Description:
 */
public class DefaultTopic implements Topic {
	private String topicName;
	private List<Queue> queues = new ArrayList<Queue>();
    public DefaultTopic(String topicName) {
    	this.topicName = topicName;
    }
    
	@Override
    public List<Queue> getQueues() {
        return queues;
    }
	@Override
	public void registerQueue(Queue queue) {
		this.queues.add(queue);
	}
	@Override
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
    
    
}
