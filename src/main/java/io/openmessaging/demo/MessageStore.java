package io.openmessaging.demo;

import io.openmessaging.Message;

public class MessageStore {

    private static final int QUEUE_SIZE = 10;
    private static final int TOPIC_SIZE = 10;

    private static ObjectIOResolver[] Qresolvers = null;
    private static ObjectIOResolver[] Tresolvers = null;

    static {
        Qresolvers = new ObjectIOResolver[QUEUE_SIZE];
        Tresolvers = new ObjectIOResolver[TOPIC_SIZE];
        for (int i = 0; i < QUEUE_SIZE; i++) {
            Qresolvers[i] = new ObjectIOResolver();
        }
        for (int i = 0; i < TOPIC_SIZE; i++) {
            Tresolvers[i] = new ObjectIOResolver();
        }
    }


    /**
     * 自定义存储逻辑
     */
    public void putQueueMessage(String path,String queue, Message message) {
        int channel = fhash(queue, QUEUE_SIZE);
        Qresolvers[channel].push(path+"queue"+channel,new DataPackage(queue, message));
    }


    /**
     * 自定义存储逻辑
     */
    public void putTopicMessage(String path,String topic, Message message) {
        int channel = fhash(topic, TOPIC_SIZE);
        Tresolvers[channel].push(path+"topic"+channel,new DataPackage(topic, message));
    }

    /**
     * 自定义哈希
     */
    public int fhash(String s, int c) {
        return (s.hashCode() % c + c) % c;
    }
}
