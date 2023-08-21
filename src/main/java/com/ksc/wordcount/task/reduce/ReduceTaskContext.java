package com.ksc.wordcount.task.reduce;

import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskContext;

public class ReduceTaskContext extends TaskContext {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    ReduceFunction reduceFunction;
    PartionWriter partionWriter;

    public ReduceTaskContext(String applicationId, String stageId, int taskId, int partionId,ShuffleBlockId[] shuffleBlockId, ReduceFunction reduceFunction,PartionWriter partionWriter) {
        super(applicationId, stageId, taskId, partionId);
        this.shuffleBlockId = shuffleBlockId;
        //this.destDir = destDir;
        this.reduceFunction = reduceFunction;
        this.partionWriter = partionWriter;
    }

    public ShuffleBlockId[] getShuffleBlockId() {
        return shuffleBlockId;
    }

    //public String getDestDir() {
//        return destDir;
//    }

    public ReduceFunction getReduceFunction() {
        return reduceFunction;
    }

    public PartionWriter getPartionWriter() {
        return partionWriter;
    }

}
