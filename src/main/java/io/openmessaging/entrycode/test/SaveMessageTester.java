package io.openmessaging.entrycode.test;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.entrycode.impl.NIOProducer;
import io.openmessaging.entrycode.util.Constant;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nbs on 2017/5/28.
 */
public class SaveMessageTester {
    public static void main(String[] args) {
        int maxDataNum = 10000;
        KeyValue properties = new DefaultKeyValue();
         /*
         //实际测试时利用 STORE_PATH 传入存储路径
         //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
          */
        properties.put("STORE_PATH", Constant.STORE_PATH);

        // test modify
        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        Producer producer = new NIOProducer(properties);

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右.
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右.
        String queue1 = "QUEUE1"; //实际测试时大概会有100个Queue左右
        String queue2 = "QUEUE2"; //实际测试时大概会有100个Queue左右
        List<Message> messagesForTopic1 = new ArrayList<>(1024);
        List<Message> messagesForTopic2 = new ArrayList<>(1024);
        List<Message> messagesForQueue1 = new ArrayList<>(1024);
        List<Message> messagesForQueue2 = new ArrayList<>(1024);
        for (int i = 0; i < maxDataNum; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1,  (topic1).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2).getBytes()));
        }
        System.out.println("start");
        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        for (int i = 0; i < maxDataNum; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        System.out.println("end");

    }
}
