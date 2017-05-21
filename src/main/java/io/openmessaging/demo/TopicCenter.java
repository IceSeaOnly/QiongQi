package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by IceSea on 2017/5/21.
 * GitHub: https://github.com/IceSeaOnly
 */
public class TopicCenter extends Thread{
    private static FileReadThread fileReadThread;
    private static TopicCenter topicCenter;
    private static LinkedBlockingDeque<DataPackage> dataPackages;
    private static LinkedBlockingDeque<String> pathTrans = new LinkedBlockingDeque<>();

    public static TopicCenter getInstance(){
        return topicCenter;
    }

    public static void setPath(String p){
        pathTrans.add(p);
    }

    static {
        topicCenter = new TopicCenter();
        topicCenter.start();
    }

    private static HashMap<String,ArrayList<DataPackage>> topicMessages;

    @Override
    public void run() {
        super.run();
        topicMessages = new HashMap<>();
        try {
            String path = pathTrans.take();
            ReadFile(path);
            while (true){
                DataPackage dt = dataPackages.take();
                if(!topicMessages.containsKey(dt.getName())){
                    topicMessages.put(dt.getName(),new ArrayList<>());
                }
                topicMessages.get(dt.getName()).add(dt);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void ReadFile(String path) {
        File topicRoot = new File(path+"t/");
        List<String> ts = Arrays.asList(topicRoot.list());
        ArrayList<String> readList = new ArrayList<>();
        for (int i = 0; i < ts.size(); i++) {
            if(ts.get(i).contains("-0")){
                String name = ts.get(i);
                int sum = 0;
                for (int j = 0; j < ts.size(); j++) {
                    if(ts.get(i).startsWith(name+"-"))
                        readList.add(name+(sum++));
                }
            }
        }
        Collections.sort(readList,new NameSorter());
        dataPackages = new LinkedBlockingDeque<>();
        fileReadThread = new FileReadThread(readList,dataPackages);
        fileReadThread.start();
    }


}
