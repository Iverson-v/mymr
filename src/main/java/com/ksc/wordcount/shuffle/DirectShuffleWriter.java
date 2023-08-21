package com.ksc.wordcount.shuffle;

import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;//

    int reduceTaskNum;

    ObjectOutputStream[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds ;

    public DirectShuffleWriter(String baseDir,String shuffleId,String  applicationId,String stageId,int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;//"/tmp/shuffle"  shuffle文件放哪里
        this.reduceTaskNum = reduceTaskNum;//2
        fileWriters = new ObjectOutputStream[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        for (int i = 0; i < reduceTaskNum; i++) {//for循环两次
            try {
                //这里的i训练两次
                shuffleBlockIds[i]=new ShuffleBlockId(baseDir,applicationId,shuffleId,stageId,mapId,i);
                //创建文件夹，表示shuffle文件的位置，"/tmp/shuffle/applicationId"在这个文件夹下生成shuffle文件
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs();
                fileWriters[i] = new ObjectOutputStream(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));
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
            fileWriters[next.getKey().hashCode()%reduceTaskNum].writeObject(next);
        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                fileWriters[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public  MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId,shuffleBlockIds);
    }


}
