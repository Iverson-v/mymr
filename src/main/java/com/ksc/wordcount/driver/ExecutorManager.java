package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExecutorManager {

    /**
     * ExecutorUrl和ExecutorRegister的映射
     */
    private Map<String, ExecutorRegister> executorRegisterMap = new HashMap<>();

    /**
     * ExecutorUrl和Core数的映射
     */
    private Map<String, Integer> executorAvailableCoresMap = new HashMap<>();

    public void updateExecutorRegister(ExecutorRegister executorRegister) {
        //1.把executor端的地址和executorRegister类当作键值对放到map中。表示注册成功
        executorRegisterMap.put(executorRegister.getExecutorUrl(),executorRegister);
        //2.建立ExecutorUrl和Core数的映射
        executorAvailableCoresMap.put(executorRegister.getExecutorUrl(),executorRegister.getCores());
    }

    public Map<String, Integer> getExecutorAvailableCoresMap() {
        return executorAvailableCoresMap;
    }

    //获取内存中的executorRegisterMap
    public Map<String, ExecutorRegister> getExecutorRegisterMap() {
        return executorRegisterMap;
    }

    //获取该executor的最大核心
    public int getExecutorMaxCore(String executorUrl) {
        return executorRegisterMap.get(executorUrl).getCores();
    }

    //获取该executor可用核数
    public int getExecutorAvaliableCore(String executorUrl) {
        return executorAvailableCoresMap.get(executorUrl);
    }

    //可用核心更新。分配一个任务就更新可用核心
    public synchronized void  updateExecutorAvailableCores(String executorUrl,int addCores){
        int oldCore = executorAvailableCoresMap.get(executorUrl)==null?0:executorAvailableCoresMap.get(executorUrl);
        executorAvailableCoresMap.put(executorUrl,oldCore+addCores);
    }






}
