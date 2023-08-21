package com.ksc.wordcount.datasourceapi;


public interface FileFormat {

    boolean isSplitable(String filePath);

    PartionFile[] getSplits(String filePath, long size);

    PartionReader createReader();

    PartionWriter createWriter(String destPath, int partionId);



}
