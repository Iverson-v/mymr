package com.ksc.wordcount.thrift.service;


import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.UrlTopNConfig;
import com.ksc.wordcount.driver.AkkaDriver;
import com.ksc.wordcount.driver.UrlTopNDriver;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public  class ServiceImpl implements UrlTopNService.Iface {

//    @Override
//    public HelloMsg sayHello() {
//        HelloMsg helloMsg = new HelloMsg();
//        helloMsg.setMsg("Hello from the server!");
//        return helloMsg;
//    }
//
//    @Override
//    public MyResponseMsg ask(MyRequestMsg request) {
//        MyResponseMsg response = new MyResponseMsg();
//        response.setStatus(true);
//        response.setNo(request.getNo());
//        response.setMsg("Server received message: " + request.getMsg());
//        return response;
//    }

    // 模拟一个应用结果状态,一个applicationId对应一个运行状态
    private Map<String, UrlTopNAppResponse> appStatusMap = new HashMap<>();

    private Map<String ,String> outputMap=new HashMap<>();


    // 提交新的URL TopN任务，并返回任务的状态。
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) throws TException {

        //记录下applicationid和输出目录的关系。
        outputMap.put(urlTopNAppRequest.applicationId ,urlTopNAppRequest.getOuputPath());

        // 模拟接收应用请求并设置为"accepted"状态
        UrlTopNAppResponse response = new UrlTopNAppResponse();
        response.setApplicationId(urlTopNAppRequest.getApplicationId());
        response.setAppStatus(1); // 0: accepted
        // 保存到应用状态存储
        appStatusMap.put(urlTopNAppRequest.getApplicationId(), response);


        // 调用UrlTopNDriver运行

        boolean flag;
        try {
            flag=UrlTopNDriver.run(urlTopNAppRequest);
        } catch (Exception e) {
            //任务执行失败
            response.setApplicationId(urlTopNAppRequest.getApplicationId());
            response.setAppStatus(3); // 2：finished
            appStatusMap.put(urlTopNAppRequest.getApplicationId(), response);
            return response;
        }


        //todo 运行成功应该更新状态
        response.setApplicationId(urlTopNAppRequest.getApplicationId());
        response.setAppStatus(2); // 2：finished
        // 保存到应用状态存储
        appStatusMap.put(urlTopNAppRequest.getApplicationId(), response);



        return response;
    }

    //根据applicationId获取任务的状态。
    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) throws TException {
        // 从应用状态存储中获取状态
        UrlTopNAppResponse response = appStatusMap.get(applicationId);
        return response;
    }

    //根据applicationId和topN获取任务的前N个URL结果。
    @Override
    public List<UrlTopNResult> getTopNAppResult(String applicationId) throws TException {

        //从磁盘中获取
        List<UrlTopNResult> list=new ArrayList<>();
        String outputPath = outputMap.get(applicationId);

        File file = new File(outputPath+"/urlTopN.avro");


        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                // Reuse the 'record' object to reduce object instantiations
                record = dataFileReader.next(record);

                UrlTopNResult urlTopNResult = new UrlTopNResult();
                urlTopNResult.setUrl(record.get("key").toString());
                urlTopNResult.setCount(Integer.parseInt(record.get("value").toString()));
                //System.out.println(record);
                list.add(urlTopNResult);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // 返回前topN的结果
        return list;
    }
}
