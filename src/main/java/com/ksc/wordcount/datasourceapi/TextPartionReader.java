package com.ksc.wordcount.datasourceapi;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TextPartionReader implements PartionReader<String>, Serializable {

    @Override
    public Stream<String> toStream(PartionFile partionFile) throws IOException {
        return Arrays.stream(partionFile.getFileSplits())
                .flatMap(fileSplit -> {
                    try {
                        return readLinesFromFileSplit(fileSplit);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private Stream<String> readLinesFromFileSplit(FileSplit fileSplit) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(fileSplit.getFileName(), "r");
        raf.seek(fileSplit.getStart());

        Iterator<String> iter = new Iterator<String>() {
            private long currentPos = fileSplit.getStart();

            @Override
            public boolean hasNext() {
                return currentPos < fileSplit.getStart() + fileSplit.getLength();
            }

            @Override
            public String next() {
                try {
                    String line;
                    do {
                        line = raf.readLine();
                        currentPos = raf.getFilePointer();
                    } while (line == null && hasNext());
                    return line;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false)
                .onClose(() -> {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

}
