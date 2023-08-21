package com.ksc.wordcount.rpc.Driver;

import akka.actor.ActorRef;
import com.ksc.wordcount.task.TaskContext;

public class DriverRpc {

    public static void submit(String executorUrl,TaskContext taskContext){
        System.out.println("Driver端发送任务给Executor端:"+executorUrl+",taskContext:"+taskContext);

        //向executor发送任务，executor接受任务就执行。
        DriverSystem.getExecutorRef(executorUrl).tell(taskContext, ActorRef.noSender());
    }


}
