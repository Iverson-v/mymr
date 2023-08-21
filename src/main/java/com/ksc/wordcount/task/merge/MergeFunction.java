package com.ksc.wordcount.task.merge;

import com.ksc.wordcount.task.KeyValue;

import java.util.stream.Stream;


@FunctionalInterface
public interface MergeFunction<K,V,K2,V2> extends java.io.Serializable {

    public Stream<KeyValue<K,V>> merge(Stream<KeyValue<K,V>> stream);
}
