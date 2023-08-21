package com.ksc.wordcount.worker;


import com.ksc.wordcount.conf.SlaveConfigReader;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Executor {


    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        String masterConfigPath = "bin/master.conf"; // 默认路径
        String slaveConfigPath = "bin/slave.conf"; // 默认路径

        if (args.length > 0) {
            masterConfigPath = args[0]; // 如果提供了命令行参数，那么使用命令行参数作为配置文件路径
            SlaveConfigReader.SlaveConfig slaveConfig=new SlaveConfigReader.SlaveConfig();
            slaveConfig.ip=args[1];
            slaveConfig.akkaPort= Integer.parseInt(args[2]);
            slaveConfig.rpcPort= Integer.parseInt(args[3]);
            slaveConfig.memory=args[4];
            slaveConfig.cpu= Integer.parseInt(args[5]);
            ExecutorInstance.run(slaveConfig,masterConfigPath);
        }else {
            List<SlaveConfigReader.SlaveConfig> slaveConfigs = SlaveConfigReader.readSlaveConfig(slaveConfigPath);
            for (SlaveConfigReader.SlaveConfig slaveConfig: slaveConfigs) {
                //判断配置文件中是不是本地
                if (slaveConfig.ip.equals("127.0.0.1")||slaveConfig.ip.equals("localhost")){
                    ExecutorInstance.run(slaveConfig,masterConfigPath);
                } else {
                    //不是本地的话只运行和当前ip一致的executor
                    InetAddress inetAddress = InetAddress.getLocalHost();
                    String ip=inetAddress.getHostAddress();
                    System.out.println("Local IP Address : " + ip+"开启Executor服务");
                    if (ip.equals(slaveConfig.ip)){
                        ExecutorInstance.run(slaveConfig,masterConfigPath);
                    }
                }
            }
        }
    }



}
