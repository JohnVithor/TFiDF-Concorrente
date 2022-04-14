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

public class RecordConsumer implements Runnable {
    private final ParquetWriter<GenericData.Record> writer;
    private final BlockingQueue<GenericData.Record> buffer;

    public RecordConsumer(String tfidf_out_fileName,
                          Schema schema_tfidf,
                          BlockingQueue<GenericData.Record> buffer) throws IOException {
        Configuration conf = new Configuration();
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
        this.buffer = buffer;
    }

    @Override
    public void run() {
        try {
            writer.write(buffer.take());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
