package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class ReduceStatus extends TaskStatus {

    ShuffleBlockId[] shuffleBlockIds;
    public ReduceStatus(int taskId,ShuffleBlockId[] shuffleBlockIds) {
        super(taskId, TaskStatusEnum.FINISHED);
        this.shuffleBlockIds = shuffleBlockIds;
    }
    public ReduceStatus(int taskId) {
        super(taskId, TaskStatusEnum.FINISHED);
    }

    public ReduceStatus(int taskId,TaskStatusEnum taskStatus) {
        super(taskId,taskStatus);
    }

    public ReduceStatus(int taskId,TaskStatusEnum taskStatus, String errorMsg,String errorStackTrace) {
        super(taskId,taskStatus, errorMsg,errorStackTrace);
    }

    public ShuffleBlockId[] getShuffleBlockIds() {
        return shuffleBlockIds;
    }

    public void setShuffleBlockHostAndPort(String host, int port) {
        for(ShuffleBlockId shuffleBlockId:shuffleBlockIds){
            shuffleBlockId.setHostAndPort(host,port);
        }
    }
}
