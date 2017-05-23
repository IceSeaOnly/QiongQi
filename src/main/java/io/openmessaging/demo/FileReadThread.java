package io.openmessaging.demo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by IceSea on 2017/5/21.
 * GitHub: https://github.com/IceSeaOnly
 */
public class FileReadThread extends Thread {

    private ArrayList<String>filePaths;
    private LinkedBlockingDeque<DataPackage> dataPackages;

    public FileReadThread(ArrayList<String> filePaths,
                          LinkedBlockingDeque<DataPackage> dataPackages) {
        this.filePaths = filePaths;
        this.dataPackages = dataPackages;
    }

    @Override
    public void run() {
        super.run();
        System.out.println("ReadThread with "+filePaths.size()+" tasks running...");
        for (int i = 0; i < filePaths.size(); i++) {
            try {
                FileInputStream fileInputStream = new FileInputStream(filePaths.get(i));
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                DataPackage dt = (DataPackage) objectInputStream.readObject();
                dataPackages.add(dt);
                //System.out.println(filePaths.get(i)+" Success.");
            } catch (FileNotFoundException e) {
                System.out.println(filePaths.get(i)+" Failed.");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println(filePaths.get(i)+" Failed.");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println(filePaths.get(i)+" Failed.");
                e.printStackTrace();
            }
        }
        System.out.println("ReadThread with "+filePaths.size()+" tasks stop");
    }
}
