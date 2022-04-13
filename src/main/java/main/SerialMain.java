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
    static public String filename = "devel_1_000";
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
        String docs_schema_path = "src/main/resources/docs_schema.avsc";
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String docs_out_fileName = "results_serial/" + filename + "_docs_results.parquet";
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
            Schema schema_docs = new Schema.Parser().parse(new FileInputStream(docs_schema_path));
            Schema schema_tfidf = new Schema.Parser().parse(new FileInputStream(tfidf_schema_path));
            documentList = Files.lines(Paths.get(input_path))
                    .map(line -> {
                        String [] cells = line.split("\",\"");
                        return new Document(cells[1], cells[2]);
                    }).collect(Collectors.toList());//print each line

//            BufferedReader br = Files.newBufferedReader(Paths.get(input_path), StandardCharsets.UTF_8);
//            for (String line; (line = br.readLine()) != null;) {
//                String [] cells = line.split("\",\"");
//                Document d = new Document(cells[1], cells[2]);
//                documentList.add(d);
//            }
//            br.close();

            double[] terms_count_all_docs = new double[Document.vocab_size];
            int doc_len = documentList.size();
            output.println("Vocabulary Size: " + Document.vocab_size);
            output.println("Number of Documents: " + doc_len);
            Configuration conf = new Configuration();
            OutputFile out = HadoopOutputFile.fromPath(new Path(docs_out_fileName), conf);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                    <GenericData.Record>builder(out)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema_docs)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()){
                GenericData.Record record = new GenericData.Record(schema_docs);
                for (int i = 0; i < doc_len; ++i) {
                    for (Map.Entry<Integer, Double> set: documentList.get(i).getFrequency_table().entrySet()) {
                        terms_count_all_docs[set.getKey()] += 1;
                    }
                    record.put("doc_id", i);
                    record.put("value", documentList.get(i).getTitle());
                    writer.write(record);
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
            out = HadoopOutputFile.fromPath(new Path(tfidf_out_fileName), conf);
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
                for (int j = 0; j < doc_len; ++j) {
                    doc_start = Instant.now();
                    for (int i = 0; i < Document.vocab_size; ++i) {
                        double idf = Math.log(doc_len / terms_count_all_docs[i]);
                        double tf = documentList.get(j).calculateTermFrequency(i);
                        double tfidf = tf*idf;
                        if (tfidf != 0.0) {
                            record.put("term", Document.id_token_vocabulary.get(i));
                            record.put("doc_id", j);
                            record.put("value", tfidf);
                            writer.write(record);
                        }
                    }
                    documentList.set(j, null);
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
        output.println("Total Time in minutes: " + d.toMinutes());
        output.println("Total Time in seconds: " + d.toSeconds());
    }
}
