package com.ksc.wordcount.test;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import java.io.File;
public class Test7 {
    public static void main(String[] args) throws Exception {
        File file = new File("E:\\temp\\urltoptmp\\output\\urlTopN.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                // Reuse the 'record' object to reduce object instantiations
                record = dataFileReader.next(record);
                System.out.println(record);
            }
        }
    }
}
