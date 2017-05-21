package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private ArrayList<String>dataPackages;
    private LinkedBlockingDeque<Message> messages;
    private String queue;
    private String path;
    private TopicCenter topicCenter;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.dataPackages = new ArrayList<>();
        this.messages = new LinkedBlockingDeque<>();
        this.path = properties.getString("STORE_PATH");
        if(!path.endsWith("/")) path += "/";
        topicCenter = TopicCenter.getInstance();
        TopicCenter.setPath(path);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    // 重写 读取逻辑 synchronized
    @Override public  Message poll() {
        if (dataPackages.size() == 0) {
            return null;
        }
        return null;
    }
    // 重写此处，绑定queue和topic synchronized
    // 只能绑定一次且只能一个queue，并独占这个queue
    @Override public void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        //buckets.add(queueName);
        dataPackages.addAll(topics);
//        bucketList.clear();
//        bucketList.addAll(buckets);
    }
    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
