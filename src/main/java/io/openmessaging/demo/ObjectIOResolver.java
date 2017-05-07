package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Administrator on 2017/5/6.
 * 对象存盘辅助工具
 * 存盘同步
 */
public class ObjectIOResolver {
    private ArrayList<DataPackage> dataPackages;
    private int index; //文件计数器
    private Lock lock = new ReentrantLock();
    private int MAXSIZE = 200;

    public ObjectIOResolver() {
        dataPackages = new ArrayList<>();
        index = 0;
    }

    public void push(String filePath,DataPackage msg){
        dataPackages.add(msg);
        if(dataPackages.size() > MAXSIZE){
            lock.lock();
            if(dataPackages.size() > MAXSIZE){
                new FileSaveThread(dataPackages,filePath+"-"+(index++)).start();
                dataPackages = new ArrayList<>();
            }
            lock.unlock();
        }
    }

    /**
     * 序列化读文件
     * */
    public ArrayList<DataPackage> read(String fileName){
        try {
            FileInputStream freader = new FileInputStream(fileName);
            ObjectInputStream objectInputStream = new ObjectInputStream(freader);
            ArrayList<DataPackage> msgs = (ArrayList<DataPackage>) objectInputStream.readObject();
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
