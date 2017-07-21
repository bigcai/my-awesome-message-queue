package io.openmessaging.entrycode.impl;


import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.entrycode.entity.CurrentPage;
import io.openmessaging.entrycode.entity.QueueData;
import io.openmessaging.entrycode.entity.QueueIndex;
import io.openmessaging.entrycode.interfaces.FileDataLoader;
import io.openmessaging.entrycode.interfaces.FileInit;
import io.openmessaging.entrycode.interfaces.MessageStoreInterface;
import io.openmessaging.entrycode.interfaces.SerializationInterface;
import io.openmessaging.entrycode.util.CapacityUtil;
import io.openmessaging.entrycode.util.Constant;
import io.openmessaging.entrycode.util.MappedUtil;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by nbs on 2017/5/6.
 */
public class NIOMessageStore implements MessageStoreInterface {
    private static volatile NIOMessageStore INSTANCE;
    private static String path;

    private static int runTarget = 0;
    public static int RUN_TO_WRITE = 1;
    public static int RUN_TO_READ = 2;

    private static MappedByteBuffer queueIndexBuffer;
    //pageid为key，对应的queueData内存文件映射为value
    private static ConcurrentHashMap<Integer, MappedByteBuffer> queueDataBuffer;
    //pageid为key，对应的messageData内存文件映射为value
    private static ConcurrentHashMap<Integer, MappedByteBuffer> messageDataBuffer;

    //队列名称为key，对应的多个queueindex实体为对象
    private static ConcurrentHashMap<String, List<QueueIndex>> queueIndexsMap;

    private static FileInit fileInit;
    private static FileDataLoader fileDataLoader;

    private static volatile CurrentPage currentQueueDataPage; //***-queue_data页游标
    private static volatile CurrentPage currentQueueIndexPage; //***-queue_Index页游标
    private static volatile CurrentPage currentMessageDataPage; //***-message_data页游标


    public static SerializationInterface serializationInterface = new MySerialization();
    //public static SerializationInterface serializationInterface = new JavaSerializationImpl();

    public static NIOMessageStore getInstance(String path, int runTarget) {
        if (INSTANCE == null) {
            synchronized (NIOMessageStore.class) {
                if (INSTANCE == null) {
                    NIOMessageStore.runTarget = runTarget;
                    if (NIOMessageStore.runTarget  == NIOMessageStore.RUN_TO_WRITE) {
                        //FileUtil.delFile(path); // 清除残留
                    }
                    INSTANCE = new NIOMessageStore(path);
                }
            }
        }
        return INSTANCE;
    }

    private NIOMessageStore(String path) {
        this.path = path.endsWith("/") ? path : path + "/";
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void init() throws IOException {
        fileInit = new NIOFileInit(path);
        //检查是否有落盘文件，没有则是第一次启动，重新初始化文件
        fileInit.initFilesIfIndexFileNotExist();
        fileDataLoader = new NIOFileLoader(path);

        //只在写的时候有用，用于记录地址块用到哪了
        currentQueueDataPage = new CurrentPage(1, 0);
        currentQueueIndexPage = new CurrentPage(1, 0);
        currentMessageDataPage = new CurrentPage(1, 0);

        //建立落盘文件的内存映射
        queueIndexBuffer = fileDataLoader.loadQueueIndexFiles();
        queueDataBuffer = fileDataLoader.loadQueueDataFiles();
        messageDataBuffer = fileDataLoader.loadMessageDataFiles();
        queueIndexsMap = fileDataLoader.loadQueueIndex(queueIndexBuffer);

		/*setMaxOffSet(queueIndexsMap);//初始化index和queueData的当前位置游标*/
        
        // 开启情况缓存调度器
        runScheduledToClose();
    }

    @Override
    public void putMessage(String bucket, Message message) {
        try {
            saveMessage(bucket, message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    //public synchronized Message pullMessage(String queue, String bucket) { 无需加锁，以及加了局部锁
    public  synchronized Message pullMessage(String queue, String bucket, ConcurrentHashMap<String, CurrentPage> currentPages)   {
        List<QueueIndex> myQueue = queueIndexsMap.get(queue);
        if (myQueue != null && myQueue.size() != 0) {
            // 不加锁访问Queue
            QueueData queueData = null;
            try {
                queueData = readQueueData(myQueue);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //读的时候是无限循环的，有可能这部分是没有数据的，反序列化的queueData就是属性值全为0
           // if (!(queueData == null || (queueData.getStartPosition() == 0 && queueData.getEndPosition() == 0))) {
            if (queueData != null && queueData.getEndPosition() != 0) {
            	Message message = null;
                 try {
                     message = readMessageInMessagePage(queueData);
                 } catch (IOException e) {
                     e.printStackTrace();
                 }
                 return message;
            } 
            
        } 
        if (bucket != null  ) {
            List<QueueIndex> topic = queueIndexsMap.get(bucket);
            // 加锁同步访问Topic
            QueueData queueData = null;
            if (topic != null && topic.size() != 0) { 
            	synchronized (topic) {
                	if (topic == null || topic.size() == 0) {
                    	//该topic用完
                		return null;
                	}
                	try {
                    	queueData = readQueueData(topic, currentPages);
                	} catch (IOException e) {
                		e.printStackTrace();
                	}
            	}
            	if (queueData != null && queueData.getEndPosition() != 0) {
            		Message message = null;
                	try {
                    	message = readMessageInMessagePage(queueData);
                	} catch (IOException e) {
                		e.printStackTrace();
                	}
                	return message;
            	}
            }
        } 
        // 该queue和topic已经用完
        return null;
    }

    private synchronized QueueData readQueueData(List<QueueIndex> topic, ConcurrentHashMap<String, CurrentPage> currentPages) throws IOException {
    	// 从队列中消费一个QueueData，如果该QueueData所在的QueueIndex已经被读完，就从List<QueueIndex>中移除
        synchronized (topic) {
        	Iterator<QueueIndex> i = topic.iterator();
        	
        	while (i.hasNext()) {
        		QueueIndex queueIndex = i.next();
        		CurrentPage currentPage = currentPages.get(queueIndex.getQueueName());
        		if (queueIndex.getPageId() != currentPage.getPageId() && (currentPage.getOffset() >= queueIndex.getCurrentReadPosition() && currentPage.getOffset() <= queueIndex.getCurrentWritePosition())){ // 找到该currentPage指向的queueIndex
        			continue;
        		} 
        		if(currentPage.getOffset() + Constant.QUEUE_DATA_LENGTH > queueIndex.getCurrentWritePosition()) { // 如果该页已经写满了，则换下一页
        			if( i.hasNext() ) {
        				 queueIndex = i.next();
        			} else { // 如果该队列用完则返回null
        				break;
        			}
        			currentPage.setPageId(queueIndex.getPageId());
        			currentPage.setOffset(queueIndex.getCurrentReadPosition());
        		} else {
        			// 读取QueueData
        			MappedByteBuffer mappedByteBuffer = queueDataBuffer.get(currentPage.getPageId());
            		if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
            			mappedByteBuffer = fileDataLoader.mappedFileByName(currentPage.getPageId() + "-" + Constant.DEFAULT_QUEUE_DATA_NAME);
            			queueDataBuffer.putIfAbsent(currentPage.getPageId(), mappedByteBuffer);
            		}
            		int startReadPosition = currentPage.getOffset();
            		byte[] dest = new byte[Constant.QUEUE_DATA_LENGTH];
            		synchronized (mappedByteBuffer) {
            			if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
            				mappedByteBuffer = fileDataLoader.mappedFileByName(currentPage.getPageId() + "-" + Constant.DEFAULT_QUEUE_DATA_NAME);
            				queueDataBuffer.putIfAbsent(currentPage.getPageId(), mappedByteBuffer);
            			}
            			mappedByteBuffer.position(startReadPosition);
            			mappedByteBuffer.get(dest);
            		}
            		currentPage.setOffset( startReadPosition + Constant.QUEUE_DATA_LENGTH );
            		currentPages.put(queueIndex.getQueueName(), currentPage);
            		return (QueueData) serializationInterface.byteArrayToObject(dest);
        		}
        		
        	}
        }
		return null;
	}

	private Message readMessageInMessagePage(QueueData queueData) throws IOException {
        // 根据queueData索引描述，从MessagePage中读取数据并反序列化
        MappedByteBuffer mappedByteBuffer = messageDataBuffer.get(queueData.getPageId());
        if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
           // MappedUtil.checkAndUmMapEndReadMessageData(messageDataBuffer);
        	mappedByteBuffer = fileDataLoader.mappedFileByName(queueData.getPageId() + "-" + Constant.DEFAULT_MESSAGE_DATA_NAME);
            messageDataBuffer.putIfAbsent(queueData.getPageId(), mappedByteBuffer);
        }
        int messageLength = queueData.getEndPosition() - queueData.getStartPosition();
        byte[] dest = new byte[messageLength];
        synchronized (mappedByteBuffer) {
        	if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
             	 mappedByteBuffer = fileDataLoader.mappedFileByName(queueData.getPageId() + "-" + Constant.DEFAULT_MESSAGE_DATA_NAME);
                 messageDataBuffer.putIfAbsent(queueData.getPageId(), mappedByteBuffer);
             }
        	mappedByteBuffer.position(queueData.getStartPosition());
            mappedByteBuffer.get(dest);
		}
        return (DefaultBytesMessage) serializationInterface.byteArrayToObject(dest);
    }

    private QueueData readQueueData(List<QueueIndex> myQueue) throws IOException {
        // 从队列中消费一个QueueData，如果该QueueData所在的QueueIndex已经被读完，就从List<QueueIndex>中移除
        	Iterator<QueueIndex> i = myQueue.iterator();
        	while (i.hasNext()) {
            
        		QueueIndex queueIndex = i.next();
        		if (!(queueIndex.getCurrentReadPosition() + Constant.QUEUE_DATA_LENGTH <= queueIndex.getCurrentWritePosition())){
        			i.remove();
        			continue;
        		} 
            
            
        		MappedByteBuffer mappedByteBuffer = queueDataBuffer.get(queueIndex.getPageId());
        		if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
        			mappedByteBuffer = fileDataLoader.mappedFileByName(queueIndex.getPageId() + "-" + Constant.DEFAULT_QUEUE_DATA_NAME);
        			queueDataBuffer.putIfAbsent(queueIndex.getPageId(), mappedByteBuffer);
        		}
        		int startReadPosition = queueIndex.getCurrentReadPosition();
        		byte[] dest = new byte[Constant.QUEUE_DATA_LENGTH];
        		synchronized (mappedByteBuffer) {
        			if (mappedByteBuffer == null) {//之前映射的文件已经部分读完
        				mappedByteBuffer = fileDataLoader.mappedFileByName(queueIndex.getPageId() + "-" + Constant.DEFAULT_QUEUE_DATA_NAME);
        				queueDataBuffer.putIfAbsent(queueIndex.getPageId(), mappedByteBuffer);
        			}
        			mappedByteBuffer.position(startReadPosition);
        			mappedByteBuffer.get(dest);
        		}
        		queueIndex.setCurrentReadPosition(startReadPosition + Constant.QUEUE_DATA_LENGTH);
        		return (QueueData) serializationInterface.byteArrayToObject(dest);
        	}
        return null;
    }

    /**
     * 写入消息
     *
     * @param bucket
     * @param message
     * @throws IOException
     */
    private void saveMessage(String bucket, Message message) throws IOException {
        byte[] messagebyte = serializationInterface.objectToByteArray(message);
        //checkIfFileFull(messagebyte.length);//检查文件是否已经满了，满了则新分配
        if (!queueIndexsMap.containsKey(bucket)) {
            queueIndexsMap.putIfAbsent(bucket, new ArrayList<QueueIndex>());
        }
        List<QueueIndex> queueIndexs = queueIndexsMap.get(bucket);
        // 写入Message之前创建QueueData
        synchronized (queueIndexs) {

            QueueIndex index = getLastQueueIndex(queueIndexsMap, message, bucket, queueIndexs);
            //QueueData queueData = createQueueDataByMessage(bucket, currentMessageDataPage.getPageId(), messagebyte.length);

            //byte[] queueDataBytes = serializationInterface.objectToByteArray(queueData);
            QueueData queueData = writeQueueData(index, bucket, messagebyte.length);

            // 同步刷新缓存
            writeMyMessage(queueData, messagebyte);
        }
    }

    /*获取最后一个queueIndex,如果没有则创建一个  */
    private QueueIndex getLastQueueIndex(ConcurrentHashMap<String, List<QueueIndex>> queueIndexsMap, Message message, String queueName, List<QueueIndex> queueIndexs) throws IOException {
        // 检测是否需要创建新的页存放QueueData,如果需要则同步重新创建新的QueueIndex并同步修改游标,一个QueueIndex对于一个xxx-queueindex页

        QueueIndex queueIndex = null;
        if (queueIndexs.size() != 0) {
            queueIndex = queueIndexs.get(queueIndexs.size() - 1);//如果已经有了，就返回最后一个
        }

        synchronized (currentQueueDataPage) {
            if (queueIndex == null || queueIndex.isFull()) {
                CapacityUtil.capacityQueueData(currentQueueDataPage, queueDataBuffer, fileInit, fileDataLoader);
                queueIndex = new QueueIndex();
                queueIndex.setPageId(currentQueueDataPage.getPageId());
                queueIndex.setStartPosition(currentQueueDataPage.getOffset());
                queueIndex.setMyPosition(currentQueueIndexPage.getOffset());
                queueIndex.setCurrentReadPosition(queueIndex.getStartPosition());
                queueIndex.setCurrentWritePosition(queueIndex.getStartPosition());
                queueIndex.setEndPosition(queueIndex.getStartPosition() + Constant.QUEUE_DATA_LENGTH * Constant.QUEUE_DATA_NUM_IN_INDEX);
                queueIndex.setQueueName(queueName);
                queueIndex.setType(message.headers().getString(MessageHeader.TOPIC) == null ? Constant.QUEUE_TYPE : 0);
                currentQueueIndexPage.setOffset(currentQueueIndexPage.getOffset() + Constant.QUEUE_INDEX_LENGTH);
                currentQueueDataPage.setOffset(currentQueueDataPage.getOffset() + Constant.QUEUE_DATA_LENGTH * Constant.QUEUE_DATA_NUM_IN_INDEX);
                queueIndexs.add(queueIndex);
            }
        }
        return queueIndex;
    }

    private void writeMyMessage(QueueData queueData, byte[] messageByte) throws IOException {
        MappedByteBuffer mappedByteBuffer = messageDataBuffer.get(queueData.getPageId());
        synchronized (mappedByteBuffer) {
            mappedByteBuffer.position(queueData.getStartPosition());
            mappedByteBuffer.put(messageByte);
        }
    }


    private QueueData writeQueueData(QueueIndex index, String queueName, int messageLength) throws IOException {
        QueueData queueData = new QueueData();
        //queueData.setQueueName(queueName);
        // 将queueData写入queueData页ID为queueDataPageId的文件中，写入的起始位置为：startWriteQueueDataPosition
        MappedByteBuffer mappedByteBuffer = queueDataBuffer.get(index.getPageId());
        int startWritePosition = index.getCurrentWritePosition();
        synchronized (mappedByteBuffer) {
            synchronized (currentMessageDataPage) {
                CapacityUtil.capacityMessageData(currentMessageDataPage, messageLength, messageDataBuffer, fileInit, fileDataLoader);
                int offSet = currentMessageDataPage.getOffset();
                queueData.setPageId(currentMessageDataPage.getPageId());
                queueData.setStartPosition(offSet);
                mappedByteBuffer.position(startWritePosition);
                mappedByteBuffer.putInt(queueName.length());
                mappedByteBuffer.put(queueName.getBytes());
                mappedByteBuffer.putInt(currentMessageDataPage.getPageId());
                mappedByteBuffer.putInt(offSet);
                mappedByteBuffer.putInt(offSet + messageLength);
                currentMessageDataPage.setOffset(offSet + messageLength);
                index.setCurrentWritePosition(startWritePosition + Constant.QUEUE_DATA_LENGTH);
            }
        }
        updateQueueIndex(index);
        return queueData;
    }

    private void updateQueueIndex(QueueIndex index) {
        //byte[] indexBytes = serializationInterface.objectToByteArray(index);
        byte[] bytes = index.getQueueName().getBytes();
        synchronized (queueIndexBuffer) {
            queueIndexBuffer.position(index.getMyPosition());
            queueIndexBuffer.putInt(bytes.length);
            queueIndexBuffer.put(bytes);
            queueIndexBuffer.putInt(index.getPageId());
            queueIndexBuffer.putInt(index.getType());
            queueIndexBuffer.putInt(index.getCurrentReadPosition());
            queueIndexBuffer.putInt(index.getCurrentWritePosition());
            queueIndexBuffer.putInt(index.getMyPosition());
            queueIndexBuffer.putInt(index.getStartPosition());
            queueIndexBuffer.putInt(index.getEndPosition());
        }
    }

    private void runScheduledToClose() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                if (NIOMessageStore.runTarget  == NIOMessageStore.RUN_TO_WRITE) {
                   MappedUtil.unMapQueueDataByFileNum(queueDataBuffer);
                   MappedUtil.unMapMessageDataByFileNum(messageDataBuffer);
                }
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间  
        service.scheduleAtFixedRate(runnable, 0, 2000, TimeUnit.MILLISECONDS);
    }

	public static ConcurrentHashMap<String, List<QueueIndex>> getQueueIndexsMap() {
		return queueIndexsMap;
	}

	public static void setQueueIndexsMap(
			ConcurrentHashMap<String, List<QueueIndex>> queueIndexsMap) {
		NIOMessageStore.queueIndexsMap = queueIndexsMap;
	}

    
}
