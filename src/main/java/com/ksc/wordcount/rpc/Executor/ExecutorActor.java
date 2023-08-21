package com.ksc.wordcount.rpc.Executor;

import akka.actor.AbstractActor;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.map.ShuffleMapTask;
import com.ksc.wordcount.task.merge.MergeTask;
import com.ksc.wordcount.task.merge.MergeTaskContext;
import com.ksc.wordcount.task.reduce.ReduceTask;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.worker.ExecutorThreadPoolFactory;

public class ExecutorActor extends AbstractActor {

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MapTaskContext.class, taskContext -> {
                    //获取driver通过rpc发送的map任务
                    System.out.println("Executor端接受到Driver端发送的map任务:"+taskContext);
                    //获取了任务，当前Executor看有没有核心，有的话就执行任务
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ShuffleMapTask(taskContext));
                })
                .match(ReduceTaskContext.class, taskContext -> {
                    //获取driver通过rpc发送的reduce任务
                    System.out.println("Executor端接受到Driver端发送的reduce任务:"+taskContext);
                    //这里会执行ReduceTask的父类task的runnable方法
                    ExecutorThreadPoolFactory.getExecutorService().submit(new ReduceTask(taskContext));
                })
                .match(MergeTaskContext.class, taskContext -> {
                    //获取driver通过rpc发送的reduce任务
                    System.out.println("Executor端接受到Driver端发送的merge任务:"+taskContext);
                    //这里会执行ReduceTask的父类task的runnable方法
                    ExecutorThreadPoolFactory.getExecutorService().submit(new MergeTask(taskContext));
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    System.out.println("unhandled message:" + message);
                })
                .build();
    }
}
