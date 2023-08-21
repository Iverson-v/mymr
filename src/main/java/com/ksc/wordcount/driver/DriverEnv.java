package com.ksc.wordcount.driver;

public class DriverEnv {
    public static String host;
    public static int port;


    //管理task,需要单例模式
    public static TaskManager taskManager = new TaskManager();

    //管理executor
    public static ExecutorManager executorManager = new ExecutorManager();

    //综合管理
    public static TaskScheduler taskScheduler = new TaskScheduler(taskManager,executorManager);


    public static void clear(){
        taskManager=new TaskManager();
        taskScheduler = new TaskScheduler(taskManager,executorManager);
    }
}
