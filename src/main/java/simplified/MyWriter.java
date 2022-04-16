package simplified;

import main.Document;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

public class MyWriter {
    private ParquetWriter<GenericData.Record> writer;
    private GenericData.Record record;

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
