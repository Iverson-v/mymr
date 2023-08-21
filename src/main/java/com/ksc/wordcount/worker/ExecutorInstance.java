package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.conf.MasterConfigReader;
import com.ksc.wordcount.conf.SlaveConfigReader;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;

import java.io.IOException;

public class ExecutorInstance {

    public static void run(SlaveConfigReader.SlaveConfig slaveConfig,String masterConfigPath){

        ExecutorEnv.host=slaveConfig.ip;//127.0.0.1
        ExecutorEnv.port=slaveConfig.akkaPort;//Executor的端口 5050
        ExecutorEnv.memory= slaveConfig.memory;//限制内存512MB
        MasterConfigReader.MasterConfig masterConfig;
        try {masterConfig = MasterConfigReader.readMasterConfig(masterConfigPath);
        } catch (IOException e) {throw new RuntimeException(e);}
        String driverIPPort= masterConfig.ip+":"+masterConfig.akkaPort; //127.0.0.1:4040
        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@"+driverIPPort+"/user/driverActor";//driver的地址。
        ExecutorEnv.core=slaveConfig.cpu;
        ExecutorEnv.executorUrl="akka.tcp://ExecutorSystem@"+ ExecutorEnv.host+":"+ExecutorEnv.port+"/user/executorActor";
        ExecutorEnv.shufflePort= slaveConfig.rpcPort;//shuffle的端口号,这个主要是executor去拿shuffle的

        new Thread(() -> {
            try {
                //异步开启netty服务端，每个executor都会有一个netty服务端。比如我的map阶段把shuffle文件放在本地，这时候需要别人连接这个netty取数据
                System.out.println("netty服务ShuffleService准备启动！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
                new ShuffleService(ExecutorEnv.shufflePort).start();
            } catch (InterruptedException e) {
                System.out.println("netty服务ShuffleService启动异常！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！");
                new RuntimeException(e);
            }
        }).start();

        //启动Excutor端的akka服务
        ActorSystem executorSystem = ExecutorSystem.newExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());

        boolean flag=true;
        while(flag){
            try{ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl,ExecutorEnv.memory,ExecutorEnv.core));
                System.out.println("注册成功！");
                flag=false;
            }catch (Exception e){
                System.out.println("deriver未启动。重新注册中");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
}
