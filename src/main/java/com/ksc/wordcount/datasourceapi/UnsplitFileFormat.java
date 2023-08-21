package com.ksc.wordcount.datasourceapi;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
//UnsplitFileFormat是每个文件切一个split。
public class UnsplitFileFormat implements FileFormat {


    //判断文件能不能切分，比如压缩的格式就有可能不能切
    @Override
    public boolean isSplitable(String filePath) {
        return false;
    }


    @Override
    public PartionFile[] getSplits(String filePath, long size) {
        //todo 学生实现 driver端切分split的逻辑
        File parentFile=new File(filePath);
        //1.判断传入的filePath是文件还是文件夹。
        if (parentFile.isFile()){
            //如果filePath是文件的话就只有一个split了。就封装PartionFile
            FileSplit[] fileSplits = {new FileSplit(filePath, 0, filePath.length())};
            PartionFile[] partionFiles = {new PartionFile(0, fileSplits)};
            return partionFiles;

        }

        //2.走到这里就表示这个filePath是一个文件夹
        List<PartionFile> partiongFileList=new ArrayList<>();
        File[] files = parentFile.listFiles();
        int partitionId=0;
        for (File file : files) {//实际一个partition对应多个split
            //遍历每一个file，这里每个file切成一个split。每个split放到一个partition中。
            FileSplit[] fileSplits = {new FileSplit(file.getAbsolutePath(), 0, file.length())};
            PartionFile partionFile = new PartionFile(partitionId, fileSplits);
            partiongFileList.add(partionFile);
            partitionId++;
        }



        return partiongFileList.toArray(new PartionFile[partiongFileList.size()]);
    }

    //读的时候按行读，每个partition中所有的split，把他们按照每行读然后交给map处理。就是map怎么读原始文件
    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
