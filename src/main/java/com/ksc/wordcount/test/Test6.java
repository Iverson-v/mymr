package com.ksc.wordcount.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.task.KeyValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class Test6 {
    public static void main(String[] args) throws FileNotFoundException {
        Kryo kryo = new Kryo();
        kryo.register(KeyValue.class);

        // 序列化
        File file = new File("E:\\temp\\urltoptmp\\shuffle\\b.data");
        Output output = new Output(new FileOutputStream(file));
        KeyValue<String, Integer> kv = new KeyValue<>("https:test", 123);
        kryo.writeObject(output, kv);
        output.close();

        // 反序列化
        Input input = new Input(new FileInputStream(file));
        KeyValue<String, Integer> deserializedKV = kryo.readObject(input, KeyValue.class);
        System.out.println(deserializedKV.getKey() + " : " + deserializedKV.getValue());
        input.close();
    }

}
