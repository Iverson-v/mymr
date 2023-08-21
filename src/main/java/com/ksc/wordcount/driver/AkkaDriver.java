package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.conf.MasterConfigReader;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;

import java.io.IOException;

public class AkkaDriver {
    public static void main(String[] args) throws IOException {

        //配置driver
        MasterConfigReader.MasterConfig masterConfig = MasterConfigReader.readMasterConfig("bin/master.conf");
        DriverEnv.host=masterConfig.ip;//127.0.0.1
        DriverEnv.port = masterConfig.akkaPort;//4040

        ActorSystem driverSystem = DriverSystem.getDriverSystem();
        ActorRef driverActorRef = driverSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());
    }
}
