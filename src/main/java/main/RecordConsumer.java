package main;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public class RecordConsumer implements Runnable {
    private final ParquetWriter<GenericData.Record> writer;
    private final Buffer<Data> buffer;
    private final Data end;

    private final GenericData.Record record;


    public RecordConsumer(String tfidf_out_fileName,
                          Schema schema_tfidf,
                          Data end,
                          Buffer<Data> buffer) throws IOException {
        Configuration conf = new Configuration();
        this.buffer = buffer;
        OutputFile out = HadoopOutputFile.fromPath(new Path(tfidf_out_fileName), conf);
        writer = AvroParquetWriter.
                <GenericData.Record>builder(out)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema_tfidf)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
        this.end = end;
        this.record = new GenericData.Record(schema_tfidf);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Data data = buffer.poll();
                record.put("term", data.term());
                record.put("doc", data.doc_id());
                record.put("value", data.value());
                if (data == end) {
                    writer.close();
                    return;
                }
                writer.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
