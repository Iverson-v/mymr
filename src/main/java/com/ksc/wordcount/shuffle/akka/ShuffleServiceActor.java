package com.ksc.wordcount.shuffle.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.pf.ReceiveBuilder;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;

public class ShuffleServiceActor extends AbstractActor {


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ShuffleBlockId.class, shuffleBlockId -> {
                    System.out.println("ShuffleServiceHandler received:"+shuffleBlockId.name());
                    File file = new File(shuffleBlockId.getShufflePath());
                    if (file.exists()) {
                        RandomAccessFile raf = new RandomAccessFile(file, "r");
                        FileChannel channel = raf.getChannel();
                        FileRegion region = new DefaultFileRegion(channel, 0, channel.size());
                        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
                        while (objectInputStream.available()>0){
                            AbstractMap.SimpleEntry simpleEntry = (AbstractMap.SimpleEntry) objectInputStream.readObject();
                            getSender().tell(simpleEntry, getSelf());
                        }
                        System.out.println("shuffle File send success");
                    } else {
                        getSender().tell("shuffle File not found: " + file.getName(), getSelf());
                    }
                })
                .match(String.class, message -> {
                    System.out.println("Server received: " + message);
                })
                .build();
    }
}
