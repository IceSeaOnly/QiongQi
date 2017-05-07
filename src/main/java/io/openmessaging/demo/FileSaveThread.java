package io.openmessaging.demo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

/**
 * Created by Administrator on 2017/5/7.
 */
public class FileSaveThread extends Thread {
    private ArrayList<DataPackage> dataPackages;
    private String fileName;

    public FileSaveThread(ArrayList<DataPackage> dataPackages, String fileName) {
        this.dataPackages = dataPackages;
        this.fileName = fileName;
    }

    @Override
    public void run() {
        super.run();
        Write(dataPackages,fileName);
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
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("END:"+fileName);
    }
}
