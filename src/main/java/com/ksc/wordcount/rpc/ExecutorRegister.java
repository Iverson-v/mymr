package com.ksc.wordcount.rpc;

import java.io.Serializable;

//Executor注册类
public class ExecutorRegister implements Serializable {

    String executorUrl;//executor自己的地址，告诉driver自己在哪里eg："akka.tcp://ExecutorSystem@"+ ExecutorEnv.host+":"+ExecutorEnv.port+"/user/executorActor";
    String memory;//内存
    int cores;//核数

    public ExecutorRegister(String executorUrl, String memory, int cores) {
        this.executorUrl = executorUrl;
        this.memory = memory;
        this.cores = cores;
    }

    public String getExecutorUrl() {
        return executorUrl;
    }
    public String getMemory() {
        return memory;
    }
    public int getCores() {
        return cores;
    }
}
