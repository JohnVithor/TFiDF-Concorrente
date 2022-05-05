package jv.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import jv.records.Data;
import jv.utils.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyWriter {

    private final ParquetWriter<GenericData.Record> writer;
    private final GenericData.Record record;

    public MyWriter (OutputFile out, Schema schema) throws IOException {
        writer = AvroParquetWriter.
                <GenericData.Record>builder(out)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
            record = new GenericData.Record(schema);
    }
    public void write(Data data) throws IOException {
        synchronized (this) {
            record.put("term",data.term());
            record.put("doc",data.doc_id());
            record.put("value",data.value());
            writer.write(record);
        }
    }

    public void close() throws IOException {
        writer.close();
    }
}
