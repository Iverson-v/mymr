package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.Driver.DriverRpc;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskScheduler {

    private TaskManager taskManager;
    private ExecutorManager executorManager ;

    /**
     * taskId和ExecutorUrl的映射,表示把这个任务分给这个executor
     */
    private Map<Integer,String> taskExecuotrMap=new HashMap<>();

    public TaskScheduler(TaskManager taskManager, ExecutorManager executorManager) {
        this.taskManager = taskManager;
        this.executorManager = executorManager;
    }

    //提交任务，分发任务给executor
    public void submitTask(int stageId) {
        //根据stageId获取任务
        BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);

        //这里只要有任务就发出去！
        while (!taskQueue.isEmpty()) {
            //todo 学生实现 轮询给各个executor派发任务
            executorManager.getExecutorAvailableCoresMap().forEach((executorUrl,availableCores)->{
                if (availableCores>0&&!taskQueue.isEmpty()){
                    //核心数大于0，而且task队列不为空表示现在可以取出这个任务分配给这个executor
                    TaskContext task = taskQueue.poll();

                    //分配这个任务给executor，就是把taskid和executorUrl放到内存中
                    taskExecuotrMap.put(task.getTaskId(),executorUrl);
                    System.out.println(executorUrl+":去执行任务："+task.getTaskId());

                    //更新这个executor的可用核心，-1
                    executorManager.updateExecutorAvailableCores(executorUrl,-1);

                    //正式提交任务，通过akka发送给executor
                    DriverRpc.submit(executorUrl,task);
                }
            });




            try {
                String executorAvailableCoresMapStr=executorManager.getExecutorAvailableCoresMap().toString();
                System.out.println("TaskScheduler submitTask stageId:"+stageId+",taskQueue size:"+taskQueue.size()+", executorAvailableCoresMap:" + executorAvailableCoresMapStr+ ",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    //等待executor完成
    public void waitStageFinish(int stageId){
        StageStatusEnum stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        while (stageStatusEnum==StageStatusEnum.RUNNING){
            try {
                System.out.println("等待任务完成 stageId:"+stageId+",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        }
        if(stageStatusEnum == StageStatusEnum.FAILED){
            //todo 之后的作业应该有个重试机制
            System.err.println("stageId:"+stageId+" failed");
            System.exit(1);
        }
    }

    //如果任务完成或者失败了，executor核数加一。
    public void updateTaskStatus(TaskStatus taskStatus){
        if(taskStatus.getTaskStatus().equals(TaskStatusEnum.FINISHED)||taskStatus.getTaskStatus().equals(TaskStatusEnum.FAILED)){
            String executorUrl=taskExecuotrMap.get(taskStatus.getTaskId());
            executorManager.updateExecutorAvailableCores(executorUrl,1);
        }
    }


}
