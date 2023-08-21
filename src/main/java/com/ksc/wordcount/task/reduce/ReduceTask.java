package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.shuffle.KryoShuffleWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReduceTask extends Task {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    ReduceFunction reduceFunction;
    PartionWriter partionWriter;



    public ReduceTask(ReduceTaskContext reduceTaskContext) {
        super(reduceTaskContext);
        this.shuffleBlockId = reduceTaskContext.getShuffleBlockId();
        //this.destDir = reduceTaskContext.getDestDir();
        this.reduceFunction = reduceTaskContext.getReduceFunction();
        this.partionWriter = reduceTaskContext.getPartionWriter();
    }




    public ReduceStatus runTask() throws Exception {

        System.out.println("Executor开始执行reduce任务:获取shuffle文件！！！！！！！！！！！！！！！！！！！！！！！！！！！！");

        //1.获取shuffle文件，通过netty客户端去拿
        ShuffleClient shuffleClient = new ShuffleClient();
        Stream<KeyValue> stream=Stream.empty();
        for(ShuffleBlockId shuffleBlockId:shuffleBlockId){
            stream=Stream.concat(stream,shuffleClient.fetchShuffleData(shuffleBlockId));
        }

        System.out.println("Executor开始执行reduce任务:reduce方法！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
        //reduce方法
        Stream reduceStream = reduceFunction.reduce(stream);

        System.out.println("Executor开始执行reduce任务:kryo序列化到磁盘方法！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
        //todo 这里新添加的
        String shuffleId= UUID.randomUUID().toString();
        //这里封装一个流。写shuffle文件的流   这里reduceTaskNum必须设置为1 ！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
        //
        KryoShuffleWriter shuffleWriter = new KryoShuffleWriter(AppConfig.shuffleTempDir, shuffleId,applicationId,stageId, partionId, 1);
        //将maptask的处理结果写入shuffle文件中
        shuffleWriter.write(reduceStream);
        //关闭shuffle流
        shuffleWriter.commit();

        System.out.println("Executor执行reduce任务成功！准备返回自己的状态给Driver!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        //这一部也很重要，封装了ReduceStatus
        return shuffleWriter.getReduceStatus(taskId);







        //todo 返回状态
        //return new ReduceStatus(super.taskId, TaskStatusEnum.FINISHED);
        }
}
