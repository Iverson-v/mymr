package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionReader;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.DirectShuffleWriter;
import com.ksc.wordcount.shuffle.KryoShuffleWriter;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.map.MapTaskContext;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

public class ShuffleMapTask extends Task<MapStatus> {

    PartionFile partiongFile;
    PartionReader partionReader;
    int reduceTaskNum;
    MapFunction mapFunction;

    public ShuffleMapTask(MapTaskContext mapTaskContext) {
        super(mapTaskContext);
        this.partiongFile = mapTaskContext.getPartiongFile();
        this.partionReader = mapTaskContext.getPartionReader();
        this.reduceTaskNum = mapTaskContext.getReduceTaskNum();//在wordcountdriver里面封装为2
        this.mapFunction = mapTaskContext.getMapFunction();
    }


    public MapStatus runTask() throws IOException {
        //1.map任务，把文件读到流中。maptask读取原始数据文件的内容  每个partition里的所有split按照行读取到allStream里面。
        Stream<String> stream = partionReader.toStream(partiongFile);


        //2.执行map方法。
        //kvStrem长这个样子  [{abc,1},{abc,1},{cdf,1}]
        Stream kvStream = mapFunction.map(stream);

        String shuffleId= UUID.randomUUID().toString();
        //这里封装一个流。写shuffle文件的流
        //
        KryoShuffleWriter shuffleWriter = new KryoShuffleWriter(AppConfig.shuffleTempDir, shuffleId,applicationId,stageId, partionId, reduceTaskNum);
        //将maptask的处理结果写入shuffle文件中
        shuffleWriter.write(kvStream);
        //关闭shuffle流
        shuffleWriter.commit();
        //更新任务状态,这里吧shuffle赋值给了mapStatus
        return shuffleWriter.getMapStatus(taskId);
    }




}
