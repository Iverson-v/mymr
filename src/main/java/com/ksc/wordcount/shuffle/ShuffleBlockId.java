package com.ksc.wordcount.shuffle;

import java.io.Serializable;

public class ShuffleBlockId implements Serializable {

    String host;//ip和port在map任务完成之后才赋值发给dirver，如果不是finish状态没必要发送ip和port
    int port;
    String shuffleBaseDir;
    String shuffleId;
    String applicationId;
    int mapId;
    int reduceId;

    String stageId;


    public ShuffleBlockId(String shuffleBaseDir,String applicationId, String shuffleId, String stageId,int mapId, int reduceId) {
        this.shuffleBaseDir = shuffleBaseDir;//    "/tmp/shuffle"
        this.applicationId = applicationId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.reduceId = reduceId;//reduceId也是生成shuffle文件的id，
        this.stageId=stageId;
    }

    public void setHostAndPort(String host,int port){
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getReduceId() {
        return reduceId;
    }



    public String name() {
        return "shuffle_" + shuffleId +"_"+stageId+"_"+mapId + "_" + reduceId;
    }

    public String getShufflePath(){
        return getShuffleParentPath()+"/"+name()+".kryo";
    }
    public String getShuffleParentPath(){
        return shuffleBaseDir+"/"+applicationId;  //  "/tmp/shuffle/applicationId"
    }
}
