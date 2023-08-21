package com.ksc.wordcount.datasourceapi;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

//一个task对应一个partition，
public class UrlTopSplitFileFormat implements FileFormat {


    //判断文件能不能切分，比如压缩的格式就有可能不能切
    @Override
    public boolean isSplitable(String filePath) {
        return true;
    }



    @Override
    public PartionFile[] getSplits(String filePath, long targetSplitSize) {
        //1.最终分成的Partition都放在list中，一个partition对应一个任务。
        List<PartionFile> partitionFileList = new ArrayList<>();
        //2.列出该文件夹下所有文件
        File parentFile = new File(filePath);
        File[] files = parentFile.listFiles();
        int partitionId = 0;
        //3.循环遍历每个文件
        for (File file : files) {
            //判断文件是否小于分片，小于就直接把整个文件放到partition数组中
            if (file.length()<=targetSplitSize){
                PartionFile partionFile =
                        new PartionFile(partitionId, new FileSplit[]{new FileSplit(file.getAbsolutePath(),0,file.length())});
                partitionFileList.add(partionFile);
                partitionId++;
                continue;
            }
            //这里表示是大文件，需要分片。
            List<FileSplit> splits = getSplitsForLargeFile(file.getAbsolutePath(), targetSplitSize);
            for (FileSplit split : splits) {
                PartionFile partionFile = new PartionFile(partitionId, new FileSplit[]{split});
                partitionFileList.add(partionFile);
                partitionId++;
            }
        }

        return partitionFileList.toArray(new PartionFile[partitionFileList.size()]);
    }

    //大文件进行分片
    private List<FileSplit> getSplitsForLargeFile(String filePath, long targetSplitSize) {
        List<FileSplit> splits = new ArrayList<>();
        File file = new File(filePath);
        long totalSize = file.length();

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long currentStartPos = 0;

            while (currentStartPos < totalSize) {
                long currentEndPos = currentStartPos + targetSplitSize - 1;

                // 如果不是文件的结尾，找到完整的行
                if (currentEndPos < totalSize) {
                    raf.seek(currentEndPos);
                    char currentChar = (char) raf.read();
                    while (currentChar != '\n' && currentChar != '\r') {
                        currentEndPos++;
                        currentChar = (char) raf.read();
                    }
                    // 如果是Windows的'\r\n'，则需要额外读取一字节
                    if (currentChar == '\r') {
                        currentEndPos++;
                    }
                } else {
                    currentEndPos = totalSize - 1;  // 对于最后一个分片，设置结束位置为文件的结尾
                }

                splits.add(new FileSplit(filePath, currentStartPos, currentEndPos - currentStartPos + 1));
                currentStartPos = currentEndPos + 1;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return splits;
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
