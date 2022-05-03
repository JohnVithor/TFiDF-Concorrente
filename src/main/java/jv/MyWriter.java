package jv;

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

    public static void main(String[] args) {
        String file = "train_id";
        Path input_path = Path.of("datasets/"+file+".csv");
        ForEachApacheUtil util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        AtomicInteger n_docs = new AtomicInteger(0);
        try(Stream<String> lines = Files.lines(input_path)) {
            Map<String, Long> count = lines
                    .parallel()
                    .peek(e -> n_docs.getAndIncrement())
                    .map(line -> util.setOfTerms(line, stopwords))
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
            ObjectMapper objectMapper = new ObjectMapper();
            MapType type = objectMapper.getTypeFactory().constructMapType(
                    Map.class, String.class, Integer.class);
            Map<String, Integer> map = objectMapper.readValue(new File(file+".json"), type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
