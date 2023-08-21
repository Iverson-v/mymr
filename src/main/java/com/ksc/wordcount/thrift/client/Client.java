package com.ksc.wordcount.thrift.client;


import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.UrlTopNConfig;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.FileNotFoundException;
import java.util.List;

public class Client {
    public static void main(String[] args)throws Exception {

        // 创建与Thrift服务器的连接
        TTransport transport = new TSocket("127.0.0.1", 5151);
        transport.open();

        // 设置通信协议为TBinaryProtocol
        TProtocol protocol = new TBinaryProtocol(transport);
        UrlTopNService.Client client = new UrlTopNService.Client(protocol);





        // 关闭连接
        transport.close();

    }
}









//    public static void main(String[] args) throws Exception {
//            // 初始化Thrift，使用TSocket 进行TCP传输，设置连接到 127.0.0.1:9091
//            TTransport transport = new TSocket("127.0.0.1", 5151);
//            transport.open();
//            // 使用TBinaryProtocol 序列化协议
//            TProtocol protocol = new TBinaryProtocol(transport);
//            UrlTopNService.Client client = new UrlTopNService.Client(protocol);
//
//            HelloMsg helloMsg = client.sayHello();
//            System.out.println("Response from remote service: " + helloMsg.getMsg());
//
//            int i = 0;
//            // 交互性调用 service.ask(string),使用 System.in.read() 读取用户输入，然后调用 service.ask(string)
//            while (true) {
//                i++;
//                System.out.print("Please input your message:");
//                int ch;
//                StringBuilder sb = new StringBuilder();
//                while ((ch = System.in.read()) != '\n') {
//                    sb.append((char) ch);
//                }
//                // sb 转换为 utf-8 string
//                String msg = new String(sb.toString().getBytes("ISO-8859-1"),"UTF-8");
//                if (msg.equals("exit")) {
//                    break;
//                }
//                MyRequestMsg requestMsg = new MyRequestMsg();
//                requestMsg.setMsg(msg);
//                requestMsg.setNo(i);
//                MyResponseMsg myResponseMsg = client.ask(requestMsg);
//                //使用 黄色字体 打印 response2
//                System.out.println("\033[33;4m" + myResponseMsg.toString() + "\033[0m");
//            }
//            transport.close();
//    }
