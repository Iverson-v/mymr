package com.ksc.wordcount.rpc.Executor;

import akka.actor.ActorRef;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.reduce.ReduceStatus;
import com.ksc.wordcount.worker.ExecutorEnv;

public class ExecutorRpc {

    //更新map任务的状态
    public static void updateTaskMapStatue(TaskStatus taskStatus){
        if (taskStatus instanceof MapStatus && ((MapStatus) taskStatus).getTaskStatus() == TaskStatusEnum.FINISHED){
            ((MapStatus) taskStatus).setShuffleBlockHostAndPort(ExecutorEnv.host,ExecutorEnv.shufflePort);
        }
        if (taskStatus instanceof ReduceStatus && ((ReduceStatus) taskStatus).getTaskStatus() == TaskStatusEnum.FINISHED){
            ((ReduceStatus) taskStatus).setShuffleBlockHostAndPort(ExecutorEnv.host,ExecutorEnv.shufflePort);
        }
        ExecutorSystem.getDriverRef().tell(taskStatus, ActorRef.noSender());
    }

    //Executor向driver发送信息。注册自己executorRegister。
    public static void register(ExecutorRegister executorRegister){
        ExecutorSystem.getDriverRef().tell(executorRegister, ActorRef.noSender());
    }
}
