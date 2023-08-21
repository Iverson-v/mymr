package com.ksc.wordcount.task.merge;

import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class MergeStatus extends TaskStatus {

    public MergeStatus(int taskId) {
        super(taskId, TaskStatusEnum.FINISHED);
    }

    public MergeStatus(int taskId,TaskStatusEnum taskStatus) {
        super(taskId,taskStatus);
    }

    public MergeStatus(int taskId,TaskStatusEnum taskStatus, String errorMsg,String errorStackTrace) {
        super(taskId,taskStatus, errorMsg,errorStackTrace);
    }

}
