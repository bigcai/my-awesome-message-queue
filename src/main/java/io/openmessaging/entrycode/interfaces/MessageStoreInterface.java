package io.openmessaging.entrycode.interfaces;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.openmessaging.Message;
import io.openmessaging.entrycode.entity.CurrentPage;

/**
 * Created by nbs on 2017/5/6.
 */
public interface MessageStoreInterface {

    public void putMessage(String bucket, Message message);//生产者send里面调用，保存message
    public Message pullMessage(String queue, String bucket, ConcurrentHashMap<String, CurrentPage> currentPages) ;  //消费者调用
}
