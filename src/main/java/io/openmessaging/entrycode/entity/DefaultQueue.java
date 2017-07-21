package io.openmessaging.entrycode.entity;

import io.openmessaging.PullConsumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * User: vk
 * Date: 2017/4/28
 * Time: 12:06
 * Description:
 */
public class DefaultQueue implements Queue {

    private PullConsumer pullConsumer = null;
    private String queueName;
    private LinkedHashMap<String, Topic> topics = new LinkedHashMap<String, Topic>();

    public DefaultQueue(PullConsumer pullConsumer, String queueName) {
    	this.pullConsumer = pullConsumer;
    	this.queueName = queueName;
    }

	@Override
    public LinkedHashMap<String, Topic> getTopics() {
        return topics;
    }

    @Override
    public PullConsumer getOwner() {
        return pullConsumer;
    }

	@Override
	public void attachTopic(Topic topic) {
		if( !topics.containsKey( topic.getTopicName() )) {
			topic.registerQueue( this );
			topics.put( topic.getTopicName(), topic);
		}
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
	
}
