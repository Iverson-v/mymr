package com.ksc.wordcount.task.map;

import java.util.stream.Stream;


@FunctionalInterface
public interface MapFunction<T,KeyValue> extends java.io.Serializable {
    Stream<KeyValue> map(Stream<T> stream);
}
