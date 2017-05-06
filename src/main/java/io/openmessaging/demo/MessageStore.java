package io.openmessaging.demo;

import io.openmessaging.Message;
import org.junit.Test;

import java.io.*;
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

   public static final int QUEUE_SIZE = 10;
   public static final int TOPIC_SIZE = 10;

   /**
    * 自定义存储逻辑
    * */
    public void putQueueMessage(String queue, Message message) {
        int channel = fhash(queue,QUEUE_SIZE);
    }


    /**
     * 自定义存储逻辑
     * */
    public void putTopicMessage(String topic, Message message) {
        int channel = fhash(topic,TOPIC_SIZE);
    }

    /**
     * 自定义哈希
     * */
    public int fhash(String s,int c){
        return (s.hashCode()%c+c)%c;
    }


    public String Serialization(Message m){
        ISMessage msg = (ISMessage) m;
        StringBuilder sb = new StringBuilder();
        sb.append(new String(msg.getBody()));
        msg.headers().keySet().forEach(
                v->sb.append("\n ").append(v).append("\n ").append(msg.headers().getString(v)));
        msg.properties().keySet().forEach(
                v->sb.append("\n  ").append(v).append("\n  ").append(msg.properties().getString(v))
        );
        return sb.toString();
    }


    /**
     * 序列化存文件
     * */
    public void Write(ArrayList<Message> msgs,String fileName){
        try {
            FileOutputStream outStream = new FileOutputStream(fileName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
            objectOutputStream.writeObject(msgs);
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 序列化读文件
     * */
    public ArrayList<Message> read(String fileName){
        try {
            FileInputStream freader = new FileInputStream(fileName);
            ObjectInputStream objectInputStream = new ObjectInputStream(freader);
            ArrayList<Message> msgs = (ArrayList<Message>) objectInputStream.readObject();
            return msgs;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
