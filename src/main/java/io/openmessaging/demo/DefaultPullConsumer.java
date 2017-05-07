package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = null;
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }


    // TODO 重写 读取逻辑
    @Override public synchronized Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }

        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
//            Message message = messageStore.pullMessage(queue, bucket);
//            if (message != null) {
//                return message;
//            }
        }
        return null;
    }
    // TODO 重写此处，绑定queue和topic
    // 只能绑定一次且只能一个queue，并独占这个queue
    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics);
        bucketList.clear();
        bucketList.addAll(buckets);
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
