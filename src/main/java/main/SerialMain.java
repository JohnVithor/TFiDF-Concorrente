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
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String filename = "test_id";
    static private final String input_path = "datasets/"+filename+".csv";
    static private final String tfidf_out_fileName = "results_serial/" + filename+ "_tfidf_results.parquet";
    static private final String log_output = "logs_serial/output_"+filename+".log";
    public static void main(String[] args) throws FileNotFoundException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {/*Descarta o log*/}
        }));
        run();
    }

    public static void run() throws FileNotFoundException {
        PrintStream output = new PrintStream(new OutputStream() {
            final FileOutputStream f = new FileOutputStream(log_output);
            @Override
            public void write(int b) throws IOException {
                f.write(b);
                System.out.write(b);
            }
        });
        Instant start = Instant.now();
        load_stop_words();
        try {
            List<Document> documentList = getDocumentList();
            output.println("Vocabulary Size: " + Document.vocab_size);
            output.println("Number of Documents: " + documentList.size());

            double[] terms_count_all_docs = getTerms_count_all_docs(documentList);
            process(documentList, terms_count_all_docs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Duration d = Duration.between(start, Instant.now());
        output.println("Total Time in nanoseconds: " + d.toNanos());
        output.println("Total Time in milliseconds: " + d.toMillis());
        output.println("Total Time in seconds: " + d.toSeconds());
        output.println("Total Time in minutes: " + d.toMinutes());
    }
    private static void load_stop_words() {
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            Document.stopwords.addAll(Arrays.stream(reader.readLine().split(",")).toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Document> getDocumentList() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(input_path))) {
            return lines.sequential()
                    .map(line -> {
                        String [] cells = line.split("\",\"");
                        return new Document(Integer.parseInt(
                                cells[0].replaceFirst("\"", "")), cells[1], cells[2]);
                    }).collect(Collectors.toList());
        }
    }

    private static double[] getTerms_count_all_docs(List<Document> documentList) {
        double[] terms_count_all_docs = new double[Document.vocab_size];
        for (Document document : documentList) {
            for (Integer key : document.getFrequency_table().keySet()) {
                terms_count_all_docs[key] += 1;
            }
        }
        return terms_count_all_docs;
    }
    public static void process(List<Document> documentList, double[] terms_count_all_docs) throws IOException {
        Configuration conf = new Configuration();
        Schema schema_tfidf = new Schema.Parser().parse(new FileInputStream(tfidf_schema_path));
        OutputFile out = HadoopOutputFile.fromPath(new Path(tfidf_out_fileName), conf);
        int doc_len = documentList.size();
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.
                <GenericData.Record>builder(out)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema_tfidf)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            GenericData.Record record = new GenericData.Record(schema_tfidf);
            for (Document doc:documentList) {
                record.put("doc", doc.getId());
                processDocument(terms_count_all_docs, doc_len, writer, record, doc);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    private static void processDocument(double[] terms_count_all_docs,
                                        int doc_len,
                                        ParquetWriter<GenericData.Record> writer,
                                        GenericData.Record record,
                                        Document doc) throws IOException {
        for (Integer key: doc.getFrequency_table().keySet()) {
            double idf = Math.log(doc_len / terms_count_all_docs[key]);
            double tf = doc.calculateTermFrequency(key);
            record.put("term", Document.id_token_vocabulary.get(key));
            record.put("value", tf*idf);
            writer.write(record);
        }
    }
}
