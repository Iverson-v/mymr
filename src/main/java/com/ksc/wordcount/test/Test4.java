package com.ksc.wordcount.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.task.KeyValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class Test4 {
    public static void main(String[] args) throws FileNotFoundException {
        Kryo kryo = new Kryo();
        File file = new File("E:\\temp\\urltoptmp\\shuffle\\a.data");
        kryo.register(KeyValue.class);  // 注册你想反序列化的类
        Input input = new Input(new FileInputStream(file)); // inputStream可以是文件流或者网络流
        KeyValue keyValue = null;

        while (true) {
            try {
                keyValue = kryo.readObject(input, KeyValue.class);
                System.out.println(keyValue.getKey()+" : "+keyValue.getValue());
                // 发送给客户端
            } catch (KryoException e) {
                // 当达到文件末尾时，Kryo可能会抛出KryoException
                break;
            }
        }
        input.close();
    }
}
