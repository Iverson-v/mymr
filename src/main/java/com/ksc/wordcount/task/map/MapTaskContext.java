package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionReader;
import com.ksc.wordcount.task.TaskContext;

public class MapTaskContext extends TaskContext {

    PartionFile partiongFile;
    PartionReader partionReader;
    int reduceTaskNum;
    MapFunction mapFunction;


    public MapTaskContext(String applicationId, String stageId, int taskId, int partionId, PartionFile partiongFile, PartionReader partionReader, int reduceTaskNum, MapFunction mapFunction) {
        super(applicationId, stageId, taskId, partionId);
        this.partiongFile = partiongFile;
        this.partionReader = partionReader;
        this.reduceTaskNum = reduceTaskNum;
        this.mapFunction = mapFunction;
    }

    public PartionFile getPartiongFile() {
        return partiongFile;
    }

    public PartionReader getPartionReader() {
        return partionReader;
    }

    public int getReduceTaskNum() {
        return reduceTaskNum;
    }

    public MapFunction getMapFunction() {
        return mapFunction;
    }

}
