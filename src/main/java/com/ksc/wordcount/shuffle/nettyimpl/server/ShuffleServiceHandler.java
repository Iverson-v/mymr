package com.ksc.wordcount.shuffle.nettyimpl.server;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.FileComplate;
import com.ksc.wordcount.task.KeyValue;
import io.netty.channel.*;


import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;


//自定义handler需要继承netty提供的adaptor
public class ShuffleServiceHandler extends ChannelInboundHandlerAdapter {

    private Kryo kryo;

    //这里可以读取客户端的消息
    //1.ChannelHandlerContext ctx，这是上下文对象，含有pipeline，channel
    //2.Object msg ：这是客户端发送的数据
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //System.out.println("netty服务端开始把shuffle文件传输给客户端:"+msg);

        //表示这个handler目前只处理ShuffleBlockId类型的消息
        if (msg instanceof ShuffleBlockId) {
            //调用处理客户端发送的请求，需要把结果给客户端。
            handleClientShuffleRequest(ctx,msg);
        } else {
            super.channelRead(ctx, msg);
        }
    }

    public void handleClientShuffleRequest(ChannelHandlerContext ctx, Object msg)throws Exception{
        ShuffleBlockId shuffleBlockId =(ShuffleBlockId) msg;
        //System.out.println("ShuffleServiceHandler received:"+((ShuffleBlockId) msg).name());
        //获取到该shuffle文件
        File file = new File(shuffleBlockId.getShufflePath());
        if (file.exists()) {

            kryo = new Kryo();
            kryo.register(KeyValue.class);  // 注册你想反序列化的类
            Input input = new Input(new FileInputStream(file)); // inputStream可以是文件流或者网络流
            KeyValue keyValue = null;

            while (true) {
                try {
                     keyValue = kryo.readObject(input, KeyValue.class);
                    //System.out.println(keyValue.getKey()+" : "+keyValue.getValue());
                    // 发送给客户端
                    ctx.writeAndFlush(keyValue);
                } catch (KryoException e) {
                    // 当达到文件末尾时，Kryo可能会抛出KryoException
                    break;
                }
            }
            input.close();

            //ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
            //Object obj = null;

//            do{
//                try{
//                    obj = objectInputStream.readObject();
//                } catch (EOFException e){
//                    break;
//                }
//                //发送给客户端
//                ctx.writeAndFlush(keyValue);
//            }while (keyValue != null);
            //System.out.println("netty服务端把shuffle文件成功传输给客户端!");

            //数据写入缓存并刷新，结束发送。
            ctx.writeAndFlush(new FileComplate());


        } else {
            ctx.writeAndFlush("shuffle File not found: " + file.getAbsolutePath());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}