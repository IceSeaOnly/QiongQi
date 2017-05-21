package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.Thread.sleep;

public class DefaultProducer  implements Producer,MessageFactory {

    private static final int MAXSIZE_EACH_FILE = 5000;
    private FileSaveThread fileSaveThread;
    private LinkedBlockingDeque<DataPackage> dataPackages;
    private KeyValue properties; //这里包含文件存放路径
    private String FILEPATH = null; //
    private Map<String,Integer> dataCounter; // 文件存储计数器
    private ArrayList<String> topicInitedNameList; // topic计数器初始化标志器
    private ArrayList<String> queueInitedNameList; // queue计数器初始化标志器
    private Map<String,ArrayList<Message>> queues;
    private Map<String,ArrayList<Message>> topics;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        FILEPATH = properties.getString("STORE_PATH");
        if(!FILEPATH.endsWith("/"))
            FILEPATH += "/";
        dataPackages = new LinkedBlockingDeque<>();
        dataCounter = new HashMap<>();
        queues = new HashMap<>();
        topics = new HashMap<>();
        topicInitedNameList = new ArrayList<>();
        queueInitedNameList = new ArrayList<>();
        fileSaveThread = new FileSaveThread(dataPackages);
        fileSaveThread.start();
    }

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        ISMessage defaultBytesMessage = new ISMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }
    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        ISMessage defaultBytesMessage = new ISMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }

    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        // 重写存储逻辑，高效存储

        if(topic == null){
            // to queue
            if(!queueInitedNameList.contains(queue)){
                queueInitedNameList.add(queue);
                queues.put(queue,new ArrayList<>());
                dataCounter.put(queue,0);
            }
            queues.get(queue).add(message);
            if(queues.get(queue).size() > MAXSIZE_EACH_FILE){
                int index = dataCounter.get(queue);
                dataCounter.put(queue,index+1);
                dataPackages.add(new DataPackage(0,FILEPATH+"q/",queue+"-"+index,queues.get(queue)));
                queues.put(queue,new ArrayList<>());
            }
        }else{
            // to topic
            if(!topicInitedNameList.contains(topic)){
                topicInitedNameList.add(topic);
                topics.put(topic,new ArrayList<>());
                dataCounter.put(topic,0);
            }
            topics.get(topic).add(message);
            if(topics.get(topic).size() > MAXSIZE_EACH_FILE){
                int index = dataCounter.get(topic);
                dataCounter.put(topic,index+1);
                dataPackages.add(new DataPackage(1,FILEPATH+"t/",topic+"-"+index,topics.get(topic)));
                topics.put(topic,new ArrayList<>());
            }
        }
    }

    @Override
    public void flush() {
        for (int i = 0; i < topicInitedNameList.size(); i++) {
            String topic = topicInitedNameList.get(i);
            if(topics.get(topic).size() > 0){
                int index = dataCounter.get(topic);
                dataPackages.add(new DataPackage(1,FILEPATH+"t/",topic+"-"+index,topics.get(topic)));
            }
        }
        for (int i = 0; i < queueInitedNameList.size(); i++) {
            String queue = queueInitedNameList.get(i);
            if(queues.get(queue).size() > 0){
                int index = dataCounter.get(queue);
                dataPackages.add(new DataPackage(0,FILEPATH+"q/",queue+"-"+index,queues.get(queue)));
            }
        }
        while (!dataPackages.isEmpty()){
            try {
                sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }



    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }


}
