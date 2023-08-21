package com.ksc.wordcount.conf;
import com.ksc.urltopn.thrift.UrlTopNAppRequest;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;



public class UrlTopNConfig {

    public static List<UrlTopNAppRequest> parseConfigFile(String filePath) throws FileNotFoundException {
        List<UrlTopNAppRequest> configList = new ArrayList<>();

        Scanner scanner = new Scanner(new File(filePath));

        // 跳过标题行
        if (scanner.hasNextLine()) {
            scanner.nextLine();
        }

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();

            if (!line.isEmpty()) {
                String[] parts = line.split("\\s+");
                String applicationId = parts[0];
                String inputDirectory = parts[1];
                String outputDirectory = parts[2];
                int topN = Integer.parseInt(parts[3]);
                int reduceTaskCount = Integer.parseInt(parts[4]);
                int partitionSizeBytes = Integer.parseInt(parts[5]);

                configList.add(new UrlTopNAppRequest(applicationId, inputDirectory, outputDirectory, topN, reduceTaskCount, partitionSizeBytes));
            }
        }

        scanner.close();

        return configList;
    }

    public static void main(String[] args) throws FileNotFoundException {
        List<UrlTopNAppRequest> configs = parseConfigFile("bin/urltopn.conf");
        for (UrlTopNAppRequest config : configs) {
            System.out.println(config);
        }
    }
}
