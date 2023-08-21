package com.ksc.wordcount.task.merge;


import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.client.ShuffleClient;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceStatus;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.util.stream.Stream;

public class MergeTask extends Task {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    MergeFunction mergeFunction;
    PartionWriter partionWriter;



    public MergeTask(MergeTaskContext mergeTaskContext) {
        super(mergeTaskContext);
        this.shuffleBlockId = mergeTaskContext.getShuffleBlockId();
        //this.destDir = reduceTaskContext.getDestDir();
        this.mergeFunction = mergeTaskContext.getMergeFunction();
        this.partionWriter = mergeTaskContext.getPartionWriter();
    }




    public MergeStatus runTask() throws Exception {

        System.out.println("Executor开始执行merge任务:获取shuffle文件！！！！！！！！！！！！！！！！！！！！！！！！！！！！");

        //1.获取shuffle文件，通过netty客户端去拿
        Stream<KeyValue> stream=Stream.empty();
        for(ShuffleBlockId shuffleBlockId:shuffleBlockId){
            stream=Stream.concat(stream,new ShuffleClient().fetchShuffleData(shuffleBlockId));
        }


        System.out.println("Executor开始执行merge任务:merge方法！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
        //mrge方法
        Stream mergeStream = mergeFunction.merge(stream);

        //reduceStream.forEach(keyValue-> System.out.println(keyValue));

        System.out.println("Executor开始执行merge任务:avro持久化到磁盘方法！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
        //写道最终的output文件
        partionWriter.write(mergeStream);



        System.out.println("Executor执行merge任务成功！准备返回自己的状态给Driver!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        //返回状态
        return new MergeStatus(super.taskId, TaskStatusEnum.FINISHED);
    }
}
