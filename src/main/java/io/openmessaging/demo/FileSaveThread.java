package io.openmessaging.demo;

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
    private LinkedBlockingDeque<ArrayList<DataPackage>> dataPackages;
    private String fileName;
    private int counter;


    public FileSaveThread(LinkedBlockingDeque<ArrayList<DataPackage>> dataPackages) {
        this.dataPackages = dataPackages;
        this.counter = 0;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void run() {
        super.run();
        while (true){
            try {
                Write(dataPackages.take(),fileName+(counter++));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 序列化存文件
     * */
    public void Write(ArrayList<DataPackage> msgs, String fileName){
        System.out.println("BEGIN:"+fileName);
        try {
            FileOutputStream outStream = new FileOutputStream(fileName);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);
            objectOutputStream.writeObject(msgs);
            outStream.flush();
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("END:"+fileName);
    }
}
