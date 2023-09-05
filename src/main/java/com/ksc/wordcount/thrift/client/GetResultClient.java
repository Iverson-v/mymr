package com.ksc.wordcount.thrift.client;

import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;

public class GetResultClient {
    public static void main(String[] args) throws Exception {
        // 创建与Thrift服务器的连接
        TTransport transport = new TSocket("127.0.0.1", 5151);
        transport.open();

        // 设置通信协议为TBinaryProtocol
        TProtocol protocol = new TBinaryProtocol(transport);
        UrlTopNService.Client client = new UrlTopNService.Client(protocol);


        List<UrlTopNResult> application0 = client.getTopNAppResult("application_1234");
        application0.stream().forEach(System.out::println);


        // 关闭连接
        transport.close();
    }

}
