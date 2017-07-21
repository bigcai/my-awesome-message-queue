package io.openmessaging.entrycode.entity;

import java.util.List;

/**
 * Created by caisz on 2017/4/28.
 */
public interface Topic {
    /**
     * 获取订阅Topic的Queue
     * @return
     */
    List<Queue> getQueues();
    
    /**
     * 注册Queue
     * @return
     */
    void registerQueue(Queue queue);
    
    /**
     * 获取Topic名称
     */
    String getTopicName();
}
