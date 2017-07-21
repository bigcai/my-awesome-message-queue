package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.util.ObjectUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    // bucket -> [ message ]
    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    // queue -> ( bucket, offset )
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    // 避免触发两次HashMap查找
    public synchronized void putMessage(String bucket, Message message) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);

        if(ObjectUtil.isNull(bucketList)){
            bucketList = new ArrayList<>(1024);
            messageBuckets.put(bucket, bucketList);
        }

        bucketList.add(message);
    }

   public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }
}
