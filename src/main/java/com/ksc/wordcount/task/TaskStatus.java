package com.ksc.wordcount.task;

public class TaskStatus implements java.io.Serializable{
    int taskId;
    TaskStatusEnum taskStatus ;
    String errorMsg;
    String errorStackTrace;

    public TaskStatus(int taskId, TaskStatusEnum taskStatus) {
        this.taskStatus = taskStatus;
        this.taskId = taskId;
    }

    public TaskStatus(int taskId,TaskStatusEnum taskStatus, String errorMsg,String errorStackTrace) {
        this.taskId = taskId;
        this.taskStatus = taskStatus;
        this.errorMsg = errorMsg;
        this.errorStackTrace = errorStackTrace;
    }

    public int getTaskId() {
        return taskId;
    }

    public TaskStatusEnum getTaskStatus() {
        return taskStatus;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public String getErrorStackTrace() {
        return errorStackTrace;
    }
}
