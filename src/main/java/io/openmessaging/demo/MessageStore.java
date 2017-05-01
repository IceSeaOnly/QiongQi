package io.openmessaging.demo;

import io.openmessaging.Message;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private static String path; //本地存储位置

    public static void setPath(String spath) {
        if(path == null)
            MessageStore.path = spath;
    }

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
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

   /**
    * 自定义存储逻辑
    * */
    public void putQueueMessage(String queue, Message message) {

    }

    /**
     * 自定义存储逻辑
     * */
    public void putTopicMessage(String topic, Message message) {

    }

    public String Serialization(Message m){
        ISMessage msg = (ISMessage) m;
        StringBuilder sb = new StringBuilder();
        sb.append("body ").append(new String(msg.getBody()));
        return sb.toString();
    }

    @Test
    public void test(){
        Message m = new ISMessage("helfffflo".getBytes());
        m.headers().put("hello",10086);
        m.headers().put("fuck",6666L);
        System.out.println(Serialization(m));
    }
}
