package io.openmessaging.demo;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

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
    private static HashMap<String,Integer> topicIndexs;

    public DataPackage getDataPackage(String topicName,int index){
        if(topicMessages.containsKey(topicName)){
            if(topicMessages.get(topicName).size() > index)
                return topicMessages.get(topicName).get(index);
            else if(topicIndexs.get(topicName) <= index)
                return new DataPackage(-1,"","",null);
        }
        return null;
    }

    @Override
    public void run() {
        super.run();
        topicMessages = new HashMap<>();
        topicIndexs = new HashMap<>();
        try {
            String path = pathTrans.take();
            ReadFile(path);
            while (true){
                DataPackage dt = dataPackages.take();
                String dname = dt.getName().split("-")[0];
                if(!topicMessages.containsKey(dname)){
                    topicMessages.put(dname,new ArrayList<>());
                }
                topicMessages.get(dname).add(dt);
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
                String name = ts.get(i).substring(0,ts.get(i).length()-2);
                int sum = 0;
                for (int j = 0; j < ts.size(); j++) {
                    if(ts.get(j).startsWith(name+"-")){
                        String fname = path+"t/"+name+"-"+(sum++);
                        readList.add(fname);
                    }
                }
                topicIndexs.put(name,sum);
            }
        }
        Collections.sort(readList,new NameSorter());
        dataPackages = new LinkedBlockingDeque<>();
        fileReadThread = new FileReadThread(readList,dataPackages);
        fileReadThread.start();
    }


}
