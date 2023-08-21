package com.ksc.wordcount.rpc.Driver;

import akka.actor.AbstractActor;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.merge.MergeStatus;
import com.ksc.wordcount.task.reduce.ReduceStatus;

public class DriverActor extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MapStatus.class, mapStatus -> {//接收到TaskStatus.class类型就处理。
                    System.out.println("Driver端接受到Executor的Map完成请求:"+mapStatus);
                    if(mapStatus.getTaskStatus() == TaskStatusEnum.FAILED) {//如果Excutor执行失败了。报错
                        System.err.println("task status taskId:"+mapStatus.getTaskId());
                        System.err.println("task status errorMsg:"+mapStatus.getErrorMsg());
                        System.err.println("task status errorStackTrace:\n"+mapStatus.getErrorStackTrace());
                    }
                    //更新任务状态
                    System.out.println("Driver更新任务状态为finished");
                    DriverEnv.taskManager.updateTaskStatus(mapStatus);
                    //如果任务完成了，任务核心加一
                    DriverEnv.taskScheduler.updateTaskStatus(mapStatus);
                })
                .match(ReduceStatus.class, reduceStatus -> {//接收到TaskStatus.class类型就处理。
                    System.out.println("Driver端接受到Executor的Reduce完成请求:"+reduceStatus);
                    if(reduceStatus.getTaskStatus() == TaskStatusEnum.FAILED) {//如果Excutor执行失败了。报错
                        System.err.println("task status taskId:"+reduceStatus.getTaskId());
                        System.err.println("task status errorMsg:"+reduceStatus.getErrorMsg());
                        System.err.println("task status errorStackTrace:\n"+reduceStatus.getErrorStackTrace());
                    }
                    //更新任务状态
                    System.out.println("Driver更新任务状态为finished");
                    DriverEnv.taskManager.updateTaskStatus(reduceStatus);
                    //如果任务完成了，任务核心加一
                    DriverEnv.taskScheduler.updateTaskStatus(reduceStatus);
                })
                .match(MergeStatus.class, mergeStatus -> {//接收到TaskStatus.class类型就处理。
                    System.out.println("Driver端接受到Executor的Merge完成请求:"+mergeStatus);
                    if(mergeStatus.getTaskStatus() == TaskStatusEnum.FAILED) {//如果Excutor执行失败了。报错
                        System.err.println("task status taskId:"+mergeStatus.getTaskId());
                        System.err.println("task status errorMsg:"+mergeStatus.getErrorMsg());
                        System.err.println("task status errorStackTrace:\n"+mergeStatus.getErrorStackTrace());
                    }
                    //更新任务状态
                    System.out.println("Driver更新任务状态为finished");
                    DriverEnv.taskManager.updateTaskStatus(mergeStatus);
                    //如果任务完成了，任务核心加一
                    DriverEnv.taskScheduler.updateTaskStatus(mergeStatus);
                })
                .match(ExecutorRegister.class, executorRegister -> {//接受executor的注册请求。
                    System.out.println("Driver端接收到了executor的注册请求:"+executorRegister);
                    //注册这个executor，把executor端的地址和executorRegister类当作键值对放到map中。表示注册成功。
                    DriverEnv.executorManager.updateExecutorRegister(executorRegister);
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    System.err.println("unhandled message:" + message);
                })
                .build();
    }
}
