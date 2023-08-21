package com.ksc.wordcount.task;

import com.ksc.wordcount.avro.AvroKeyValue;

import java.util.stream.Stream;

public class KeyValueToAvroConverter {
//    public Stream<AvroKeyValue> convert(Stream<KeyValue<String, Integer>> keyValueStream) {
//        return keyValueStream.map(this::toAvroKeyValue);
//    }
    public Stream<AvroKeyValue> convert(Stream<KeyValue> keyValueStream) {
        return keyValueStream.map(this::toAvroKeyValue);
    }

    private AvroKeyValue toAvroKeyValue(KeyValue<String, Integer> keyValue) {
        return new AvroKeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static void main(String[] args) {
        Stream<KeyValue> keyValueStream = Stream.of(
                new KeyValue<>("one", 1),
                new KeyValue<>("two", 2),
                new KeyValue<>("three", 3)
        );

        KeyValueToAvroConverter converter = new KeyValueToAvroConverter();
        Stream<AvroKeyValue> avroStream = converter.convert(keyValueStream);

        // Print to see results
        avroStream.forEach(avroKeyValue ->
                System.out.println(avroKeyValue.getKey() + ": " + avroKeyValue.getValue())
        );
    }


}
