package com.ksc.wordcount.shuffle.nettyimpl.client;


import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import com.ksc.wordcount.task.KeyValue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.stream.Stream;


//客户端的handler
public class ShuffleClientHandler extends SimpleChannelInboundHandler{

    BlockingQueueStream<KeyValue> blockingQueueStream = new BlockingQueueStream<>(2);

    public ShuffleClientHandler() {
    }


    public Stream getStream() {
        return blockingQueueStream.stream();
    }


    //当通道准备就绪时候会触发
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // out = new FileOutputStream(fileName);
        //System.out.println("channelActive");
    }

    //当通道有读的数据时候触发
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        //接受服务端发送的client请求。
        //System.out.println("netty客户端接受到shuffle数据: " + msg);
        if (msg instanceof KeyValue){
            //这是我们想要的数据，存在内存中
            KeyValue entry = (KeyValue) msg;
            //这么方式数据是放在内存中的，如果数据量大，会导致内存溢出
            blockingQueueStream.add(entry);
        }
        if (msg instanceof String) {
            //如果读的是字符串返回错误，
            System.out.println("receive error: " + msg);
        }
        if (msg instanceof FileComplate) {
            //如果读到FileComplate说明发送完毕
            blockingQueueStream.done();
            ctx.close();
        }
    }

    //处理异常
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        blockingQueueStream.done();
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        //System.out.println("ShuffleClientHandler channelInactive");
        blockingQueueStream.done();
    }
}
