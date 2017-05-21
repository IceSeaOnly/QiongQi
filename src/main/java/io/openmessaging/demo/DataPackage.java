package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Administrator on 2017/5/6.
 * 数据包
 */
public class DataPackage implements Serializable{
    private static final long serialVersionUID = -7913413026245975933L;
    private int type; //0 for queue,1 for topic
    private String path;
    private String name;
    private ArrayList<Message> msgs;

    public DataPackage(int type, String path, String name, ArrayList<Message> msgs) {
        this.type = type;
        this.path = path;
        this.name = name;
        this.msgs = msgs;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Message> getMsgs() {
        return msgs;
    }

    public void setMsgs(ArrayList<Message> msgs) {
        this.msgs = msgs;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
