package com.ksc.wordcount.task;

import java.io.Serializable;

public class TaskContext implements Serializable {

    String applicationId;
    String stageId;
    int taskId;
    int partionId;

    public TaskContext(String applicationId, String stageId, int taskId, int partionId) {
        this.applicationId = applicationId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.partionId = partionId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getStageId() {
        return stageId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getPartionId() {
        return partionId;
    }



}
