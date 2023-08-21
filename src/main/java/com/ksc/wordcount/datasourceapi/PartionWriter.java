package com.ksc.wordcount.datasourceapi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Stream;

public interface PartionWriter<T>   {

    void write(Stream<T> stream) throws IOException;

}
