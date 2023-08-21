package com.ksc.wordcount.task;

public enum TaskStatusEnum {
    RUNNING(0),FINISHED(1),FAILED(2);
    private int status;
    TaskStatusEnum(int status){
        this.status = status;
    }
    public int getStatus(){
        return status;
    }
}
