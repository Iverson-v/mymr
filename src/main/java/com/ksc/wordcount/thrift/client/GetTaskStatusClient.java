package com.ksc.wordcount.thrift.client;

import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;

public class GetTaskStatusClient {
    public static void main(String[] args) throws Exception {
        // 创建与Thrift服务器的连接
        TTransport transport = new TSocket("127.0.0.1", 5151);
        transport.open();

        // 设置通信协议为TBinaryProtocol
        TProtocol protocol = new TBinaryProtocol(transport);
        UrlTopNService.Client client = new UrlTopNService.Client(protocol);

        getStatus(client,"application_0" );

        // 关闭连接
        transport.close();
    }

    public static void getStatus( UrlTopNService.Client client,String applicationId) throws TException, InterruptedException {
        while (true) {
            //获取状态
            UrlTopNAppResponse appStatus = client.getAppStatus(applicationId);
            //System.out.println(appStatus);
            if(appStatus==null){
                System.out.println("没有找到当前application");
                break;
            }

            if (appStatus.getAppStatus() == 2) {
                //finish,执行完成。
                //获取结果
                System.out.println("任务：" + applicationId + "    执行成功！");
                List<UrlTopNResult> topNAppResult = client.getTopNAppResult(applicationId);
                System.out.println("任务：" + applicationId + "结果获取如下：");
                topNAppResult.stream().forEach(System.out::println);
                break;
            }
            if (appStatus.getAppStatus() == 3) {
                //failed
                System.out.println("任务：" + applicationId + "    执行失败！");
                break;
            }
            System.out.println("任务正在执行中！");
            Thread.sleep(2000);
        }
    }
}
