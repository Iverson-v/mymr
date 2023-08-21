package com.ksc.wordcount.conf;

import javax.annotation.sql.DataSourceDefinition;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MasterConfigReader {


    public static class MasterConfig {
        public  String ip;
        public  int akkaPort;
        public  int thriftPort;
        public  String memory;

        @Override
        public String toString() {
            return "SlaveConfig{" +
                    "ip='" + ip + '\'' +
                    ", akkaPort=" + akkaPort +
                    ", thriftPort=" + thriftPort +
                    ", memory=" + memory +
                    '}';
        }
    }


    public static MasterConfig readMasterConfig(String filePath) throws IOException {
        MasterConfig masterConfig = new MasterConfig();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("#")) { // 跳过注释行
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length == 4) {
                        masterConfig.ip = parts[0];
                        masterConfig.akkaPort = Integer.parseInt(parts[1]);
                        masterConfig.thriftPort = Integer.parseInt(parts[2]);
                        masterConfig.memory = parts[3];
                    }
                }
            }
            return masterConfig;
        }
    }


    public static void main(String[] args) throws IOException {
        MasterConfig masterConfig = MasterConfigReader.readMasterConfig("bin/master.conf");
        System.out.println("IP: " + masterConfig.ip);
        System.out.println("Akka Port: " + masterConfig.akkaPort);
        System.out.println("Thrift Port: " + masterConfig.thriftPort);
        System.out.println("Memory: " + masterConfig.memory);
    }
}
