package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by Administrator on 2017/5/7.
 */
public class FileSaveThread extends Thread {
    private LinkedBlockingDeque<DataPackage> dataPackages;
    private int counter;


    public FileSaveThread(LinkedBlockingDeque<DataPackage> dataPackages) {
        this.dataPackages = dataPackages;
        this.counter = 0;
    }

    @Override
    public void run() {
        super.run();
        while (true){
            try {
                DataPackage dp = dataPackages.take();
                //System.out.println(dp.getName()+"->"+dp.getMsgs().size());
                Write(dp,dp.getPath(),dp.getName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 序列化存文件
     * */
    public void Write(DataPackage dp,String path,String fileName){
        try {
            File f = new File(path);
            if(!f.exists()) f.mkdirs();
            FileOutputStream outStream = new FileOutputStream(path+fileName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
            objectOutputStream.writeObject(dp);
            outStream.flush();
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
