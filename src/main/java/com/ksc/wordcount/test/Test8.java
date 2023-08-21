package com.ksc.wordcount.test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.conf.MasterConfigReader;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;
import com.ksc.wordcount.worker.ExecutorEnv;

import java.io.IOException;
import java.time.Duration;

public class Test8 {
    public static void main(String[] args) throws IOException {

        DriverEnv.host="127.0.0.1";//127.0.0.1
        DriverEnv.port = 4040;//4040
        ActorSystem driverSystem = DriverSystem.getDriverSystem();
        ActorRef driverActorRef = driverSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());



        ExecutorEnv.host="127.0.0.1";//127.0.0.1
        ExecutorEnv.port=5050;//Executor的端口 5050
        //启动Excutor端的akka服务1
        ActorSystem executorSystem = ExecutorSystem.newExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());


        ExecutorEnv.host="127.0.0.1";//127.0.0.1
        ExecutorEnv.port=5051;//Executor的端口 5050
        //启动Excutor端的akka服务2
        ActorSystem executorSystem1 = ExecutorSystem.newExecutorSystem();
        ActorRef clientActorRef1 = executorSystem1.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef1.path().toString());






        String executorUrl0="akka.tcp://ExecutorSystem@"+"127.0.0.1:"+5050+"/user/executorActor";
        //把自己注册到driver端。
        ActorRef executorRef = driverSystem.actorSelection(executorUrl0)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        System.out.println(executorRef);

        String executorUrl1="akka.tcp://ExecutorSystem@"+ "127.0.0.1:"+5051+"/user/executorActor";
        ActorRef executorRef1 = driverSystem.actorSelection(executorUrl1)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        System.out.println(executorRef1);
    }
}
