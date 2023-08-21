package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.avro.AvroKeyValue;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.KeyValueToAvroConverter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.http.util.ByteArrayBuffer;

import java.io.*;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private String destDest;
    private int partionId;

    public TextPartionWriter(String destDest,int partionId){
         this.destDest = destDest;//outputPath,最终的结果生成的位置  "/tmp/output";
         this.partionId = partionId;
    }

    //把partionId 前面补0，补成length位
    public String padLeft(int partionId,int length){
        String partionIdStr = String.valueOf(partionId);
        int len = partionIdStr.length();
        if(len<length){
            for(int i=0;i<length-len;i++){
                partionIdStr = "0"+partionIdStr;
            }
        }
        return partionIdStr;
    }

    //todo 学生实现 将reducetask的计算结果写入结果文件中
    @Override
    public void write(Stream<KeyValue> stream) throws IOException {
        //    "/tmp/output/part_001.txt"
        //File file=new File(destDest+"/"+"part_"+padLeft(partionId,3)+".txt");
//        File file=new File(destDest+"/"+"urlTopN.txt");
//        try(FileOutputStream fos=new FileOutputStream(file)){
//            //一行一行写到最终文件中。   abc   15
//            stream.forEach(keyValue -> {
//                try {
//                    fos.write((keyValue.getKey()+"\t"+keyValue.getValue()+"\n").getBytes("utf-8"));
//                } catch (IOException e) {throw new RuntimeException(e);}
//            });
//        }
        //调用Stream<KeyValue>转换为Stream<AvroKeyValue>的方法
        KeyValueToAvroConverter converter = new KeyValueToAvroConverter();
        Stream<AvroKeyValue> avroStream = converter.convert(stream);

        //最终urltopn文件输出地址
        new File(destDest).mkdir();//先创建文件夹
        File file = new File(destDest + "/" + "urlTopN.avro");

        //avro序列化到磁盘
        DatumWriter<AvroKeyValue> keyValueDatumWriter = new SpecificDatumWriter<>(AvroKeyValue.class);
        DataFileWriter<AvroKeyValue> dataFileWriter = new DataFileWriter<>(keyValueDatumWriter);
        dataFileWriter.create(AvroKeyValue.getClassSchema(), file);
        avroStream.forEach(avroKeyValue -> {
            try {
                dataFileWriter.append(avroKeyValue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        dataFileWriter.close();
    }

}
