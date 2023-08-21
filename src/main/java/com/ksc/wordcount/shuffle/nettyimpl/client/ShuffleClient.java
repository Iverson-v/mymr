package com.ksc.wordcount.shuffle.nettyimpl.client;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.AbstractMap;
import java.util.stream.Stream;

public class ShuffleClient {

    //static  Bootstrap clientBootstrap = getNettyBootStrap();




//    public  Bootstrap getNettyBootStrap(){
//        //1.客户端需要一个事件循环组
//        EventLoopGroup group = new NioEventLoopGroup();
//        Bootstrap b = new Bootstrap();
//        //2.设置线程组
//        b.group(group);
//        return b;
//    }
    public Stream<KeyValue> fetchShuffleData(ShuffleBlockId shuffleBlockId) throws InterruptedException {
        ShuffleClientHandler shuffleClientHandler = new ShuffleClientHandler();
        System.out.println("Bootstrap bootstrapChannel = getNettyBootStrap().channel(NioSocketChannel.class)");

        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        //2.设置线程组
        b.group(group);

        Bootstrap bootstrapChannel = b.channel(NioSocketChannel.class)//设置客户端的通道实现类
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        //加入处理器！！！！！！！！！！
                        ch.pipeline().addLast(
                                new ObjectEncoder(),
                                new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
                                shuffleClientHandler
                        );
                    }
                });


        //启动客户端去连接服务器
        ChannelFuture channelFuture = bootstrapChannel.connect(shuffleBlockId.getHost(), shuffleBlockId.getPort()).sync();
        channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    //System.out.println("客户端连接netty服务端: 连接服务器成功");
                } else {
                    System.out.println("connect File Server: 连接服务器失败!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                }
            });
        //告诉shuffle服务端我想要shuffle，发送请求在这里。

        channelFuture.channel().writeAndFlush(shuffleBlockId);
        System.out.println("connect File Server: 已发送文件请求");
        //channelFuture.channel().closeFuture().sync();
        //关闭通道进行监听
        channelFuture.channel().closeFuture();

        //group.shutdownGracefully();

        //得到我们想要的结果shuffle。
        return shuffleClientHandler.getStream();
    }
}
