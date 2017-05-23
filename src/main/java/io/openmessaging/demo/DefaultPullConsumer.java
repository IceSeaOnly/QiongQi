package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.Thread.sleep;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private ArrayList<String> topics;
    private ArrayList<Integer> dataIndexs;
    private LinkedBlockingDeque<DataPackage> messages;
    private String queue;
    private String path;
    private TopicCenter topicCenter;
    private FileReadThread fileReadThread;
    private DataPackage curDataPackage;
    private int queueSize;
    private int point = 0;


    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.topics = new ArrayList<>();
        this.dataIndexs = new ArrayList<>();
        this.messages = new LinkedBlockingDeque<>();
        this.path = properties.getString("STORE_PATH");
        if (!path.endsWith("/")) path += "/";
        topicCenter = TopicCenter.getInstance();
        TopicCenter.setPath(path);
    }


    @Override
    public KeyValue properties() {
        return properties;
    }


    // 重写 读取逻辑 synchronized
    @Override
    public Message poll() {
        if (curDataPackage == null || point >= curDataPackage.getMsgs().size()) {
            point = 0;
            curDataPackage = getQueue();
            if (curDataPackage == null) {
                boolean find = false;
                boolean could = true;
                while (!find && could) {
                    could = false;
                    for (int i = 0; i < dataIndexs.size(); i++) {
                        if (dataIndexs.get(i) != -1) {
                            could = true;
                            curDataPackage = topicCenter.getDataPackage(topics.get(i), dataIndexs.get(i));
                            if (curDataPackage != null && curDataPackage.getType() == -1) {
                                dataIndexs.set(i, -1);
                                curDataPackage = null;
                            } else if (curDataPackage != null) {
                                dataIndexs.set(i, dataIndexs.get(i) + 1);
                                find = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return curDataPackage == null ? null : curDataPackage.getMsgs().get(point++);
    }

    public DataPackage getQueue() {
        if (queueSize > 0) {
            try {
                DataPackage it = null;
                it = messages.take();
                if (it != null) queueSize--;
                return it;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // 重写此处，绑定queue和topic synchronized
    // 只能绑定一次且只能一个queue，并独占这个queue
    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        setQueue(queueName);
        //buckets.add(queueName);
        List<String> ls = (List<String>) topics;
        for (int i = 0; i < ls.size(); i++) {
            if (!this.topics.contains(ls.get(i))) {
                this.topics.add(ls.get(i));
                dataIndexs.add(0);
            }
        }
//        topics.addAll(topics);
//        bucketList.clear();
//        bucketList.addAll(buckets);
    }

    public void setQueue(String q) {
        this.queue = q;
        File root = new File(path + "q/");
        String[] ls = root.list();
        ArrayList<String> arr = new ArrayList<>();
        for (int i = 0; i < ls.length; i++) {
            if (ls[i].startsWith(q + "-"))
                arr.add(path + "q/" + ls[i]);
        }
        queueSize = arr.size();
        fileReadThread = new FileReadThread(arr, messages);
        fileReadThread.start();
    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
