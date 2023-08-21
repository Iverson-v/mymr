package com.ksc.wordcount.task.merge;

import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.TaskContext;

public class MergeTaskContext extends TaskContext {

    ShuffleBlockId[] shuffleBlockId;
    //String destDir;
    MergeFunction mergeFunction;
    PartionWriter partionWriter;

    public MergeTaskContext(String applicationId, String stageId, int taskId, int partionId,
                            ShuffleBlockId[] shuffleBlockId, MergeFunction mergeFunction, PartionWriter partionWriter) {
        super(applicationId, stageId, taskId, partionId);
        this.shuffleBlockId = shuffleBlockId;
        //this.destDir = destDir;
        this.mergeFunction = mergeFunction;
        this.partionWriter = partionWriter;
    }

    public ShuffleBlockId[] getShuffleBlockId() {
        return shuffleBlockId;
    }

    //public String getDestDir() {
//        return destDir;
//    }

    public MergeFunction getMergeFunction() {
        return mergeFunction;
    }

    public PartionWriter getPartionWriter() {
        return partionWriter;
    }

}
