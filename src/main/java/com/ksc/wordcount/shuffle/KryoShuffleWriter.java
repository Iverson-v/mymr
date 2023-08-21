package com.ksc.wordcount.shuffle;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;



public class KryoShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;//

    int reduceTaskNum;

    //ObjectOutputStream[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds ;


    private Output[] fileWriters;
    private Kryo kryo;
    //mapId=
    public KryoShuffleWriter(String baseDir, String shuffleId, String  applicationId,String stageId, int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;//"/tmp/shuffle"  shuffle文件放哪里
        this.reduceTaskNum = reduceTaskNum;//2
        fileWriters = new Output[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        kryo = new Kryo();
        kryo.register(KeyValue.class);  // 注册KeyValue类，使Kryo能够识别并优化它
        for (int i = 0; i < reduceTaskNum; i++) {//for循环两次
            try {
                //这里的i训练两次
                shuffleBlockIds[i]=new ShuffleBlockId(baseDir,applicationId,shuffleId,stageId,mapId,i);
                //创建文件夹，表示shuffle文件的位置，"/tmp/shuffle/applicationId"在这个文件夹下生成shuffle文件
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                fileWriters[i] = new Output(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //todo 学生实现 将maptask的处理结果写入shuffle文件中
    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        Iterator<KeyValue> iterator= entryStream.iterator();
        while (iterator.hasNext()){
            KeyValue next=iterator.next();
            //shuffle文件生成了，这里是聚合，把相同的{abc,1}放到一个文件中
            int index = (next.getKey().hashCode() & Integer.MAX_VALUE) % reduceTaskNum;
            kryo.writeObject(fileWriters[index], next); // 使用Kryo序列化对象
            //fileWriters[next.getKey().hashCode()%reduceTaskNum].writeObject(next);
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            fileWriters[i].close();
        }
    }

    public MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId,shuffleBlockIds);
    }

    public ReduceStatus getReduceStatus(int reduceTaskId) {
        return new ReduceStatus(reduceTaskId,shuffleBlockIds);
    }


}
