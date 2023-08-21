package com.ksc.wordcount.task;

import com.ksc.wordcount.rpc.Executor.ExecutorRpc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

public abstract class Task<T>  implements Runnable {

    protected String applicationId;
    protected String stageId;
    protected int taskId;
    protected int partionId;

    public Task(TaskContext taskContext) {
        this.applicationId = taskContext.getApplicationId();
        this.stageId = taskContext.getStageId();
        this.taskId = taskContext.taskId;
        this.partionId = taskContext.getPartionId();
    }

    //执行这里的runable方法。
    public void run() {
        try{
            //1.告诉driver端自己的任务状态为runnable
            ExecutorRpc.updateTaskMapStatue(new TaskStatus(taskId,TaskStatusEnum.RUNNING));
            //2.实现他的子类执行的方法，ShuffleMapTask和ReduceTask的runTask方法，会返回一个状态已完成
            TaskStatus taskStatus = runTask();
            //3.通过rpc向driver汇报已经完成
            ExecutorRpc.updateTaskMapStatue(taskStatus);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();
            System.err.println("task："+taskId+" failed：" );
            e.printStackTrace();
            TaskStatus taskStatus = new TaskStatus(taskId,TaskStatusEnum.FAILED,e.getMessage(),stackTrace);
            ExecutorRpc.updateTaskMapStatue(taskStatus);
            Thread.currentThread().interrupt();
        }
    }

    public abstract TaskStatus runTask() throws Exception;


}
