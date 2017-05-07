package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer  implements Producer,MessageFactory {

    private MessageStore messageStore = null;

    private KeyValue properties; //这里包含文件存放路径
    private String FILEPATH = null;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        FILEPATH = properties.getString("STORE_PATH");
        if(!FILEPATH.endsWith("/"))
            FILEPATH += "/";
        messageStore = new MessageStore();
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
        //TODO 重写存储逻辑，高效存储

        if(topic == null)
            messageStore.putQueueMessage(FILEPATH,queue, message);
        else
            messageStore.putTopicMessage(FILEPATH,topic, message);
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
