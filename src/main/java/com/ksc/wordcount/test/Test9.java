package com.ksc.wordcount.test;

import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.ExecutorRegister;

import java.util.Map;

public class Test9 {
    public static void main(String[] args) {
        Map<String, ExecutorRegister> executorRegisterMap =
                DriverEnv.executorManager.getExecutorRegisterMap();
        executorRegisterMap.forEach((s,o)->{
            System.out.println(s);
            System.out.println(o);;
        });
        System.out.println(executorRegisterMap.size());
    }
}
