package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.demobackup.ClientOMSException;
import io.openmessaging.entrycode.entity.CurrentPage;
import io.openmessaging.entrycode.entity.DefaultQueue;
import io.openmessaging.entrycode.entity.DefaultTopic;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.entity.Topic;
import io.openmessaging.entrycode.impl.NIOMessageStore;
import io.openmessaging.entrycode.interfaces.MessageStoreInterface;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStoreInterface messageStore;
    private KeyValue properties;
    //private String queue;
    private DefaultQueue queue;
    private ConcurrentHashMap<String, CurrentPage> currentPages = new ConcurrentHashMap<String, CurrentPage>();
    
    public static ConcurrentHashMap<DefaultPullConsumer, ConcurrentHashMap<String, CurrentPage>> AllConsumerCurrentPages = new ConcurrentHashMap<DefaultPullConsumer, ConcurrentHashMap<String, CurrentPage>>();
    // private Set<String> buckets = new HashSet<>();
    //private List<String> bucketList = new ArrayList<>();
    
    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        messageStore = NIOMessageStore.getInstance(properties.getString("STORE_PATH"), NIOMessageStore.RUN_TO_READ);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        if ( queue == null ) {
            return null;
        } else {
        	//use Round Robin
        	LinkedHashMap<String, Topic> topicsAttached = queue.getTopics();
            Object[]  arr =  topicsAttached.values().toArray();
            int checkNum = 0;
            while (++checkNum <= topicsAttached.size()) {
                Integer i = ((++lastIndex) % (topicsAttached.size()));
                Topic topic = (Topic)arr[i];
                String bucket = topic.getTopicName();
                Message message = messageStore.pullMessage(queue.getQueueName(), bucket, currentPages);
                if (message != null) {
                    return message;
                }
            }
        }
        return null;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public  void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = new DefaultQueue( this, queueName );
        for (String topicName : topics) {
        	// 双向绑定
        	queue.attachTopic(new DefaultTopic( topicName ));
        	
        		List<QueueIndex> indexs = NIOMessageStore.getQueueIndexsMap().get(topicName);
        	synchronized (indexs) {
        		CurrentPage currentPage = currentPages.getOrDefault(topicName, new CurrentPage(1, 0));
        		Iterator<QueueIndex> i = indexs.iterator();
            	while (i.hasNext()) {
            		QueueIndex queueIndex = i.next();
            		currentPage.setPageId(queueIndex.getPageId());
            		currentPage.setOffset(queueIndex.getCurrentReadPosition());
            		currentPages.put(topicName, currentPage);
            		break;
            	}
			}
    		
		}
        DefaultPullConsumer.AllConsumerCurrentPages.put(this, currentPages);
        // 初始化所有的CurrentPage
    }


	public DefaultQueue getQueue() {
		return queue;
	}


	public void setQueue(DefaultQueue queue) {
		this.queue = queue;
	}

    

}
