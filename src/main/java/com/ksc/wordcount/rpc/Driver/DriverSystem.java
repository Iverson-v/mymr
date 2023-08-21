package com.ksc.wordcount.rpc.Driver;

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

public class DriverSystem {

    static  ActorSystem system;

    public static ActorSystem getDriverSystem() {//单例模式，获取driver的system
        if(system!=null){
            return system;
        }
        Map<String, Object> map=new HashMap<>();

        map.put("akka.actor.provider","remote");
        map.put("akka.remote.transport","akka.remote.netty.NettyRemoteTransport");
        map.put("akka.remote.netty.tcp.hostname", DriverEnv.host);
        map.put("akka.remote.netty.tcp.port", DriverEnv.port);
        Config config = ConfigFactory.parseMap(map).withFallback(ConfigFactory.load());
        system = ActorSystem.create("DriverSystem", config);
        return system;
    }

    /**
     * executorUrl和akka连接的映射
     */
    static Map<String,ActorRef> executorRefs = new HashMap<>();

    public static ActorRef getExecutorRef(String executorUrl){//获取executor的ref
        // 如果Map中已经存在对应的executorUrl的引用，则直接返回该引用
        if(executorRefs.get(executorUrl)!=null){
            return executorRefs.get(executorUrl);
        }
        // 否则，使用Akka的actorSelection方法尝试解析executorUrl，以获取对应的ActorRef
        ActorRef executorRef = getDriverSystem().actorSelection(executorUrl)
                .resolveOne(Duration.ofSeconds(10)).toCompletableFuture().join();
        // 把新解析到的ActorRef存储到Map中，以便后续快速获取
        executorRefs.put(executorUrl,executorRef);
        // 返回新解析的ActorRef
        return executorRefs.get(executorUrl);
    }
}
