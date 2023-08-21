package com.ksc.wordcount.shuffle.nettyimpl.server;

import com.ksc.wordcount.conf.AppConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.FileWriter;

import java.util.HashMap;
import java.util.Map;

public class ShuffleService {

    int serverPort=0;

    String baseDir = AppConfig.shuffleTempDir;//shuffle文件根目录

    Map<Integer, FileWriter> map =new HashMap();

    public ShuffleService(int serverPort){
        this.serverPort=serverPort;
    }

    public void start() throws InterruptedException {
        //1.创建两个线程组，，bossgroup只处理连接请求，workergroup是真正处理请求
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        //2.创建服务端的启动对象，配置参数
        ServerBootstrap b = new ServerBootstrap().group(bossGroup, workerGroup);
        try {
                    b
                    .channel(NioServerSocketChannel.class)  //使用NioServerSocketChannel作为服务器通道实现
                    .childOption(ChannelOption.SO_KEEPALIVE, true) //设置保持活动连接状态
                    .childHandler(new ChannelInitializer<SocketChannel>() {//创建一个通道初始化对象
                        //给pipeline设置处理器
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new ObjectEncoder(),
                                    new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                    new ShuffleServiceHandler());
                        }
                    });
            //绑定端口并启动服务器
            System.out.println("netty服务端开始绑定端口：" + serverPort);
            ChannelFuture f=null;
            try{f = b.bind(serverPort).sync();}catch (Exception e){
                System.out.println("netty服务绑定端口失败，该端口已经存在！！!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!！！");
            }
            System.out.println("netty服务端绑定端口成功：" + serverPort);


            //对关闭通道进行监听
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }



}
