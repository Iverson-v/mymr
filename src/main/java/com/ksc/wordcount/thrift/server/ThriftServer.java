package com.ksc.wordcount.thrift.server;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.MasterConfigReader;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.thrift.service.ServiceImpl;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

public class ThriftServer {
    public static void main(String[] args) {
        try {
            String masterConfigPath = "bin/master.conf"; // 默认路径

            if (args.length > 0) {
                masterConfigPath = args[0]; // 如果提供了命令行参数，那么使用命令行参数作为配置文件路径
            }

            //todo 一、启动akka服务
            //配置driver
            MasterConfigReader.MasterConfig masterConfig = MasterConfigReader.readMasterConfig(masterConfigPath);
            DriverEnv.host=masterConfig.ip;//127.0.0.1
            DriverEnv.port = masterConfig.akkaPort;//4040

            //启动driver的akka服务
            ActorSystem driverSystem = DriverSystem.getDriverSystem();
            ActorRef driverActorRef = driverSystem.actorOf(Props.create(DriverActor.class), "driverActor");
            System.out.println("Driver AKKA服务启动: " + driverActorRef.path().toString());


            //todo 二、启动thrift服务端
            //创建Service的处理器，并关联到ServiceImpl的实现。
            UrlTopNService.Processor<ServiceImpl> processor = new UrlTopNService.Processor<>(new ServiceImpl());
            // 使用TServerSocket进行TCP传输，设置服务端口为9091
            TServerSocket serverSocket = new TServerSocket(masterConfig.thriftPort);
            // 使用特定的序列化协议
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            // 创建并启动Thrift多线程服务器
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverSocket).processor(processor).protocolFactory(protocolFactory));
            System.out.println("Thrift Server started on port "+masterConfig.thriftPort+"...");

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
