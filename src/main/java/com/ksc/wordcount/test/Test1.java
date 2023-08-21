package com.ksc.wordcount.test;

import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.TextPartionReader;
import com.ksc.wordcount.datasourceapi.UrlTopSplitFileFormat;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Test1 {
    public static void main(String[] args) throws IOException {
        FileFormat fileFormat = new UrlTopSplitFileFormat();
        PartionFile[]  partionFiles = fileFormat.getSplits("E:\\temp\\urltoptmp\\input", 1024);//size可以指定切多少个split
        MapFunction mapFunction = new MapFunction<String, KeyValue>() {
            @Override
            public Stream<KeyValue> map(Stream<String> stream) {
                // 正则模式匹配两种日志格式
                Pattern urlPattern = Pattern.compile("https?://([\\w-]+\\.)+[\\w-]+(/[\\w-./?%&=]*)?");


                // 匹配URL直到不允许的字符为止

                return stream.flatMap(line -> {
                    Matcher matcher = urlPattern.matcher(line);
                    List<String> urls = new ArrayList<>();
                    while (matcher.find()) {
                        urls.add(matcher.group());
                    }
                    return urls.stream();
                }).map(url -> new KeyValue(url, 1));
            }
        };

        for (PartionFile partionFile : partionFiles) {
            Stream<String> stream = fileFormat.createReader().toStream(partionFile);
//            List<String> collect1 = stream.collect(Collectors.toList());
//            for (String s :
//                    collect1) {
//                System.out.println(s);
//            }

            //stream = stream.peek(System.out::println);
            Stream<KeyValue> kvStream = mapFunction.map(stream);
            List<KeyValue> collect = kvStream.collect(Collectors.toList());
            for (KeyValue kv :
                    collect) {
                System.out.println(kv.getKey());
            }
        }
    }
//    public Stream<KeyValue> map(Stream<String> stream) {
//        Pattern urlPattern = Pattern.compile("http://[^\\s]+");
//        return stream.flatMap(line -> {
//            Matcher matcher = urlPattern.matcher(line);
//            List<String> urls = new ArrayList<>();
//            while (matcher.find()) {
//                urls.add(matcher.group());
//            }
//            return urls.stream();
//        }).map(url -> new KeyValue(url, 1));
//    }
}
