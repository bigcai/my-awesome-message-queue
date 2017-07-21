package io.openmessaging.entrycode.test;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.entrycode.impl.NIOProducer;
import io.openmessaging.entrycode.util.Constant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by nbs on 2017/6/2.
 */
public class ThreadSaveTester {
    static int maxDataNum = 100;//每个线程发送的消息数目
    static KeyValue properties = new DefaultKeyValue();

    static {
        properties.put("STORE_PATH", Constant.STORE_PATH);
    }

    static Producer producer = new NIOProducer(properties);

    public static void main(String[] args) throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(4);
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
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1, (topic1).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2, (topic2).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2).getBytes()));
        }
        long s = System.currentTimeMillis();
        new Thread(new SendMessage(messagesForTopic1,countDownLatch)).start();
        new Thread(new SendMessage(messagesForTopic2,countDownLatch)).start();
        new Thread(new SendMessage(messagesForQueue1,countDownLatch)).start();
        new Thread(new SendMessage(messagesForQueue2,countDownLatch)).start();
        countDownLatch.await();
        System.out.println(System.currentTimeMillis() - s);

    }

    public static class SendMessage implements Runnable {
        List<Message> alist;
        CountDownLatch countDownLatch;

        SendMessage(List<Message> list,CountDownLatch countDownLatch) {
            alist = list;
            this.countDownLatch =countDownLatch;
        }

        @Override
        public void run() {
            System.out.println("start " + alist.size());
            long a = System.currentTimeMillis();
            for (Message message : alist) {
                producer.send(message);
            }
            countDownLatch.countDown();
        }
    }
}
