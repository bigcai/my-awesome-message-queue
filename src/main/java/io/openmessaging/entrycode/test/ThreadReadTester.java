package io.openmessaging.entrycode.test;

import io.openmessaging.*;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultPullConsumer;
import io.openmessaging.entrycode.impl.NIOProducer;
import io.openmessaging.entrycode.util.Constant;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by nbs on 2017/6/2.
 */
public class ThreadReadTester {
    static int maxDataNum = 100;
    static KeyValue properties = new DefaultKeyValue();
    static{
        properties.put("STORE_PATH", Constant.STORE_PATH);
    }
    static  Producer producer = new NIOProducer(properties);
    //构造测试数据
    static String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右.
    static String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右.
    static String queue1 = "QUEUE1"; //实际测试时大概会有100个Queue左右
    static String queue2 = "QUEUE2"; //实际测试时大概会有100个Queue左右
    static List<Message> messagesForTopic1 =  Collections.synchronizedList(new ArrayList<Message>(1024));
    static List<Message> messagesForTopic2 =  Collections.synchronizedList(new ArrayList<Message>(1024));
    static List<Message> messagesForQueue1 =  Collections.synchronizedList(new ArrayList<Message>(1024));
    static List<Message> messagesForQueue2 =  Collections.synchronizedList(new ArrayList<Message>(1024));
    static AtomicInteger queue1Offset = new AtomicInteger(0);
    static AtomicInteger queue2Offset = new AtomicInteger(0);
    static AtomicInteger topic1Offset = new AtomicInteger(0);
    static AtomicInteger topic2Offset = new AtomicInteger(0);
    
    static AtomicInteger sum = new AtomicInteger(0);
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    
    // test modify
    //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

    public static void main(String[] args) throws InterruptedException {



        for (int i = 0; i < maxDataNum; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1,  (topic1).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2).getBytes()));
        }
        new Thread(new readMessage(1)).start();
        /*new Thread(new readMessage(2)).start();
        for (int i = 3; i <= 120; i++) {
        	 new Thread(new readMessage(i)).start();
		}*/
        countDownLatch.await();
        System.out.println(" sum : "+ sum.get());
    }

    public static class readMessage implements Runnable{
    	private int no = 0;
    	readMessage(int no) {
    		this.no = no;
    	}
        @Override
        public void run() {
        	System.out.println( "start" + this.no);

              //消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
              {
            	  if( this.no == 1 ) {
            		  PullConsumer consumer1 = new DefaultPullConsumer(properties);
            		  List<String> topics = new ArrayList<>();
                      topics.add(topic1);
                      topics.add(topic2);
                      consumer1.attachQueue(queue1, topics);

                      long startConsumer = System.currentTimeMillis();
                      int count = 0;
                      while (true) {
                          Message message = consumer1.poll();
                          if (message == null) {
                              //拉取为null则认为消息已经拉取完毕
                              break;
                          } else {
                        	  count++;
                          }
                          String topic = message.headers().getString(MessageHeader.TOPIC);
                          String queue = message.headers().getString(MessageHeader.QUEUE);
                          //实际测试时，会一一比较各个字段
                          if (topic != null) {
                              if (topic.equals(topic1)) {
                            	  topic1Offset.getAndIncrement();
                                  //Assert.assertEquals(messagesForTopic1.get(topic1Offset.getAndIncrement()), message);
                              } else {
                                  Assert.assertEquals(topic2, topic);
                                  topic2Offset.getAndIncrement();
                                  //Assert.assertEquals(messagesForTopic2.get(topic2Offset.getAndIncrement()), message);
                              }
                          } else {
                        	  
                              Assert.assertEquals(queue1, queue);
                              Assert.assertEquals(messagesForQueue1.get(queue1Offset.getAndIncrement()), message);
                          }
                      }

                      long endConsumer = System.currentTimeMillis();
                      long T2 = endConsumer - startConsumer;
                      //System.out.println("queue1Offset:"+queue1Offset+" topic1Offset:"+topic1Offset);
                      System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms %d", T2 , (queue1Offset.get() + topic1Offset.get())/T2, count));
                      sum.addAndGet(count);
                      countDownLatch.countDown();

                  } else if (this.no == 2) {
                	//消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费
                      {
                          PullConsumer consumer2 = new DefaultPullConsumer(properties);
                          List<String> topics = new ArrayList<>();
                          topics.add(topic1);
                          topics.add(topic2);
                          consumer2.attachQueue(queue2, topics);
                          long startConsumer = System.currentTimeMillis();
                          int count = 0;
                          while (true) {
                        	  Message message = consumer2.poll();
                              if (message == null) {
                                  //拉取为null则认为消息已经拉取完毕
                                  break;
                              } else {
                            	  count++;
                              }

                              String topic = message.headers().getString(MessageHeader.TOPIC);
                              String queue = message.headers().getString(MessageHeader.QUEUE);
                              //实际测试时，会一一比较各个字段
                              if (topic != null) {
                                  if (topic.equals(topic1)) {
                                	  topic1Offset.getAndIncrement();
                                      //Assert.assertEquals(messagesForTopic1.get(topic1Offset.getAndIncrement()), message);
                                  } else {
                                      Assert.assertEquals(topic2, topic);
                                      topic2Offset.getAndIncrement();
                                      //Assert.assertEquals(messagesForTopic2.get(topic2Offset.getAndIncrement()), message);
                                  }
                              } else {
                                  Assert.assertEquals(queue2, queue);
                                  Assert.assertEquals(messagesForQueue2.get(queue2Offset.getAndIncrement()), message);
                              }
                          }
                          long endConsumer = System.currentTimeMillis();
                          long T2 = endConsumer - startConsumer;
                          //System.out.println("queue2Offset:"+queue2Offset+" topic2Offset:"+topic2Offset);
                          System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms %d", T2, (queue2Offset.get() + topic2Offset.get())/T2, count));
                          sum.addAndGet(count);
                          countDownLatch.countDown();
                      }
                  } else {
                	//消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费
                      {
                          PullConsumer consumer2 = new DefaultPullConsumer(properties);
                          List<String> topics = new ArrayList<>();
                          topics.add(topic1);
                          topics.add(topic2);
                          consumer2.attachQueue("queue"+no, topics);
                          long startConsumer = System.currentTimeMillis();
                          int count = 0;
                          while (true) {
                        	  Message message = consumer2.poll();
                              if (message == null) {
                                  //拉取为null则认为消息已经拉取完毕
                                  break;
                              } else {
                            	  count++;
                              }

                              String topic = message.headers().getString(MessageHeader.TOPIC);
                              String queue = message.headers().getString(MessageHeader.QUEUE);
                              //实际测试时，会一一比较各个字段
                              if (topic != null) {
                                  if (topic.equals(topic1)) {
                                	  topic1Offset.getAndIncrement();
                                      //Assert.assertEquals(messagesForTopic1.get(topic1Offset.getAndIncrement()), message);
                                  } else {
                                      Assert.assertEquals(topic2, topic);
                                      topic2Offset.getAndIncrement();
                                      //Assert.assertEquals(messagesForTopic2.get(topic2Offset.getAndIncrement()), message);
                                  }
                              } 
                          }
                          long endConsumer = System.currentTimeMillis();
                          long T2 = endConsumer - startConsumer;
                          //System.out.println("queue2Offset:"+queue2Offset+" topic2Offset:"+topic2Offset);
                          System.out.println(String.format("Team%d cost:%d ms tps:%d q/ms %d",no, T2, (queue2Offset.get() + topic2Offset.get())/T2, count));
                          sum.addAndGet(count);
                          countDownLatch.countDown();
                      }
                  }
            	  
            }
                 
        }
    }
}
