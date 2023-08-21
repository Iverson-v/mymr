package com.ksc.wordcount.test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class Test5 {
    public static void main(String[] args) throws FileNotFoundException {
        List<KeyValue> list=new ArrayList<KeyValue>();
        list.add(new KeyValue("https:sadasda",1));
        list.add(new KeyValue("httppppppppda",1));

        File file=new File("E:\\temp\\urltoptmp\\shuffle\\a.data");
        Output output = new Output(new FileOutputStream(file));

        Kryo kryo = new Kryo();
        kryo.register(KeyValue.class);


        Stream<KeyValue> entryStream=list.stream();
        Iterator<KeyValue> iterator= entryStream.iterator();
        while (iterator.hasNext()){
            KeyValue next=iterator.next();

            kryo.writeObject(output, next); // 使用Kryo序列化对象

        }
        output.close();
    }
}
