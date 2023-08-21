package com.ksc.wordcount.driver;

public enum StageStatusEnum {
    RUNNING(0),FINISHED(1),FAILED(2);
    private int status;
    StageStatusEnum(int status){
        this.status = status;
    }
    public int getStatus(){
        return status;
    }
}
