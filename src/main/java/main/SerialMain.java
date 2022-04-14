package main;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

public class SerialMain {
    static public String filename = "devel_10_000";
    public static void main(String[] args) throws FileNotFoundException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {/*Descarta o log*/}
        }));
        PrintStream out = new PrintStream(new OutputStream() {
            final FileOutputStream f = new FileOutputStream("logs_serial/output_"+filename+".log");
            @Override
            public void write(int b) throws IOException {
                f.write(b);
                System.out.write(b);
            }
        });
        run(out);
    }

    public static void run(PrintStream output) {

        List<Document> documentList;
        String input_path = "datasets/"+filename+".csv";
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String tfidf_out_fileName = "results_serial/" + filename+ "_tfidf_results.parquet";
        Duration doc_avgduration = Duration.ZERO;
        Instant start = Instant.now();
        try(BufferedReader reader = new BufferedReader(new FileReader("datasets/stopwords.txt")))
        {
            Document.stopwords.addAll(Arrays.stream(reader.readLine().split(",")).toList());

        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Schema schema_tfidf = new Schema.Parser().parse(new FileInputStream(tfidf_schema_path));
            try (Stream<String> lines = Files.lines(Paths.get(input_path))) {
                documentList = lines
                        .map(line -> {
                            String [] cells = line.split("\",\"");
                            return new Document(cells[1], cells[2]);
                        }).collect(Collectors.toList());
            }

            double[] terms_count_all_docs = new double[Document.vocab_size];
            int doc_len = documentList.size();
            output.println("Vocabulary Size: " + Document.vocab_size);
            output.println("Number of Documents: " + doc_len);
            for (Document document : documentList) {
                for (Integer key : document.getFrequency_table().keySet()) {
                    terms_count_all_docs[key] += 1;
                }
            }

            Configuration conf = new Configuration();
            OutputFile out = HadoopOutputFile.fromPath(new Path(tfidf_out_fileName), conf);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                    <GenericData.Record>builder(out)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema_tfidf)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {
                GenericData.Record record = new GenericData.Record(schema_tfidf);
                long n = 1;
                Instant doc_start;
                for (Document doc:documentList) {
                    doc_start = Instant.now();
                    for (Integer key:doc.getFrequency_table().keySet()) {
                        double idf = Math.log(doc_len / terms_count_all_docs[key]);
                        double tf = doc.calculateTermFrequency(key);
                        record.put("term", Document.id_token_vocabulary.get(key));
                        record.put("doc", doc.getTitle());
                        record.put("value", tf*idf);
                        writer.write(record);
                    }
                    doc_avgduration = doc_avgduration.plus(
                            Duration.between(doc_start, Instant.now())
                                    .minus(doc_avgduration).dividedBy(n));
                    ++n;
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        output.println("Avg Time per Document in nanoseconds: " + doc_avgduration.toNanos());
        output.println("Avg Time per Document in milliseconds: " + doc_avgduration.toMillis());
        output.println("Avg Time per Document in seconds: " + doc_avgduration.toSeconds());
        output.println("Avg Time per Document in minutes: " + doc_avgduration.toMinutes());
        Duration d = Duration.between(start, Instant.now());
        output.println("Total Time in nanoseconds: " + d.toNanos());
        output.println("Total Time in milliseconds: " + d.toMillis());
        output.println("Total Time in seconds: " + d.toSeconds());
        output.println("Total Time in minutes: " + d.toMinutes());
    }
}
