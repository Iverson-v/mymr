package com.ksc.wordcount.test;

import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.wordcount.conf.UrlTopNConfig;
import com.ksc.wordcount.driver.UrlTopNDriver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class Test3 {
    public static void main(String[] args) throws Exception {
        List<UrlTopNAppRequest> urlTopNAppRequests = UrlTopNConfig.parseConfigFile("bin/urltopn.conf");
        UrlTopNAppRequest request = urlTopNAppRequests.get(0);
        boolean run = UrlTopNDriver.run(request);
    }
}
