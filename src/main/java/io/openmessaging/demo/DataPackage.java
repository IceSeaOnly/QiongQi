package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/5/6.
 * 数据包
 */
public class DataPackage implements Serializable{

    private static final long serialVersionUID = -3149082478050172157L;

    private String name;
    private Message message;

    public DataPackage(String name, Message message) {
        this.name = name;
        this.message = message;
    }
}
