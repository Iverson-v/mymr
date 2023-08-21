package com.ksc.wordcount.datasourceapi;

import java.io.Serializable;


public class PartionFile implements Serializable {
    private int partionId;

    //一个PartionFile对应多个FileSplit
    private FileSplit[] fileSplits;

    public PartionFile(int partionId, FileSplit[] fileSplits) {
        this.partionId = partionId;
        this.fileSplits = fileSplits;
    }

    public int getPartionId() {
        return partionId;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

}
