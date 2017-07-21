package io.openmessaging.entrycode.entity;

import io.openmessaging.PullConsumer;
import java.util.LinkedHashMap;

/**
 * Created by caisz on 2017/4/28.
 */
public interface Queue {
    /**
     * 获取订阅的Topic列表
     * @return
     */
	LinkedHashMap<String, Topic> getTopics();

    /**
     * 获取拥有该Queue的PullConsumer
     * @return
     */
    PullConsumer getOwner();
    /**
     * 绑定Topic
     */
    void attachTopic( Topic topic );
}
