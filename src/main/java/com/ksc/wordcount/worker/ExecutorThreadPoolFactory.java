package com.ksc.wordcount.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorThreadPoolFactory {

    private static ExecutorService executorService;

    public  static  ExecutorService getExecutorService() {
        if (executorService == null) {
            //线程池模拟cpu核数
            executorService = Executors.newFixedThreadPool(ExecutorEnv.core);
        }
        return executorService;
    }



}
