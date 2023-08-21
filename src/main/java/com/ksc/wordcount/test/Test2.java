package com.ksc.wordcount.test;

import java.io.*;

public class Test2 {
    public static void main(String[] args) throws IOException {


        byte[] buffer = new byte[1000];
        RandomAccessFile raf = new RandomAccessFile("E:\\temp\\urltoptmp\\input/inputFile1.log", "r");

        raf.seek(0); // 设置开始读取的位置
        int bytesRead = raf.read(buffer, 0, 152); // 读取指定长度的字节

        if (bytesRead != -1) {
            String result = new String(buffer, 0, bytesRead); // 将字节转换为字符串
            System.out.println(result);
        }

        raf.close();










    }
}
