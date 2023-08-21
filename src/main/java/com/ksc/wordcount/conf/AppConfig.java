package com.ksc.wordcount.conf;

import java.io.File;

public class AppConfig {

//    public static String shuffleTempDir = "/tmp/shuffle";
    public static File baseDir = new File(System.getProperty("java.io.tmpdir"));
    public static File shuffleDir = new File(baseDir, "shuffle");

    public static String shuffleTempDir =  shuffleDir.getAbsolutePath();//C:\Users\石浩轼\AppData\Local\Temp\shuffle
}
