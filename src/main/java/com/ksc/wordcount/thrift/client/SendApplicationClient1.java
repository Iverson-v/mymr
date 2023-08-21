package com.ksc.wordcount.thrift.client;

import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.UrlTopNConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;

public class SendApplicationClient1 {
    public static void main(String[] args) throws Exception {
        // 创建与Thrift服务器的连接
        TTransport transport = new TSocket("127.0.0.1", 5151);
        transport.open();

        // 设置通信协议为TBinaryProtocol
        TProtocol protocol = new TBinaryProtocol(transport);
        UrlTopNService.Client client = new UrlTopNService.Client(protocol);


        sendApplication(client);


        // 关闭连接
        transport.close();
    }
    public static void sendApplication(UrlTopNService.Client client) throws Exception {
        List<UrlTopNAppRequest> urlTopNAppRequests = UrlTopNConfig.parseConfigFile("bin/urltopn1.conf");
        for (int i = 0; i < urlTopNAppRequests.size(); i++) {
            UrlTopNAppRequest request = urlTopNAppRequests.get(i);
            // 调用服务方法
            UrlTopNAppResponse response = client.submitApp(request);
            System.out.println(response.getApplicationId() +":"+ response.getAppStatus());
        }
    }
}
