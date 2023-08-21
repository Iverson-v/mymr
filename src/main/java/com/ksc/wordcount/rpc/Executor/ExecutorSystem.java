package com.ksc.wordcount.rpc.Executor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.worker.ExecutorEnv;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ExecutorSystem {

    static  ActorSystem system;

    //单例模式
    public static ActorSystem newExecutorSystem() {
        //todo 修改为非单例模式
//        if(system!=null){
//            return system;
//        }
        Map<String, Object> map=new HashMap<>();

        map.put("akka.actor.provider","remote");
        map.put("akka.remote.transport","akka.remote.netty.NettyRemoteTransport");
        map.put("akka.remote.netty.tcp.hostname", ExecutorEnv.host);
        map.put("akka.remote.netty.tcp.port", ExecutorEnv.port);
        Config config = ConfigFactory.parseMap(map).withFallback(ConfigFactory.load());
        //启动akka。
        system = ActorSystem.create("ExecutorSystem", config);
        return system;
    }

    public static ActorRef getDriverRef(){//获取driver的ref
        String driverUrl = ExecutorEnv.driverUrl;//获取driver的地址
        ActorRef driverRef = system.actorSelection(driverUrl)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        return driverRef;
    }
}
