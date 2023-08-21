package com.ksc.wordcount.datasourceapi;

import java.io.IOException;
import java.util.stream.Stream;

public interface PartionReader<T>  {

    Stream<T> toStream(PartionFile partiongFile) throws IOException;
}
