package com.ksc.wordcount.conf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class SlaveConfigReader {
    public static class SlaveConfig {
        public String ip;
        public int akkaPort;
        public int rpcPort;
        public String memory;
        public int cpu;

        @Override
        public String toString() {
            return "SlaveConfig{" +
                    "ip='" + ip + '\'' +
                    ", akkaPort=" + akkaPort +
                    ", rpcPort=" + rpcPort +
                    ", memory='" + memory + '\'' +
                    ", cpu=" + cpu +
                    '}';
        }
    }

    public static void main(String[] args) {
        String filePath = "bin/slave.conf"; // Replace with the actual path
        List<SlaveConfig> configs = readSlaveConfig(filePath);

        // Print the parsed configurations
        for (SlaveConfig config : configs) {
            System.out.println(config);
        }
    }

    public static List<SlaveConfig> readSlaveConfig(String filePath) {
        List<SlaveConfig> configList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Skipping comments or empty lines
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                if (parts.length == 5) {
                    SlaveConfig config = new SlaveConfig();
                    config.ip = parts[0];
                    config.akkaPort = Integer.parseInt(parts[1]);
                    config.rpcPort = Integer.parseInt(parts[2]);
                    config.memory = parts[3];
                    config.cpu = Integer.parseInt(parts[4]);

                    configList.add(config);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return configList;
    }
}
