package com.ksc.wordcount.shuffle;

import com.ksc.wordcount.task.map.MapStatus;

import java.io.IOException;
import java.util.stream.Stream;

public  interface ShuffleWriter<KeyValue> {

    void write(Stream<KeyValue> stream) throws IOException;

    void commit();

    MapStatus getMapStatus(int mapTaskId);

}
