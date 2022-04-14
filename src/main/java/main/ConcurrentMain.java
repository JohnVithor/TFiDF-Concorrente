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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentMain {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String filename = "devel_100_000";
    static private final String input_path = "datasets/"+filename+".csv";
    static private final String tfidf_out_fileName = "results_concurrent/" + filename+ "_tfidf_results.parquet";
    static private final String log_output = "logs_concurrent/output_"+filename+".log";
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
        Duration doc_avgduration;
        try {
            List<ConcurrentDocument> documentList = getDocumentList();
            output.println("Vocabulary Size: " + ConcurrentDocument.vocab_size);
            output.println("Number of Documents: " + documentList.size());

            double[] terms_count_all_docs = getTerms_count_all_docs(documentList);
            doc_avgduration = process(documentList, terms_count_all_docs);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
    private static void load_stop_words() {
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            ConcurrentDocument.stopwords.addAll(Arrays.stream(reader.readLine().split(",")).toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<ConcurrentDocument> getDocumentList() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(input_path))) {
            return lines.parallel()
                    .map(line -> {
                        String [] cells = line.split("\",\"");
                        return new ConcurrentDocument(cells[1], cells[2]);
                    }).collect(Collectors.toList());
        }
    }

    private static double[] getTerms_count_all_docs(List<ConcurrentDocument> documentList) {
        List<Object> locks = Collections.nCopies(ConcurrentDocument.vocab_size,new Object());
        double[] terms_count_all_docs = new double[ConcurrentDocument.vocab_size];
        documentList.parallelStream().forEach(concurrentDocument -> {
            for (Integer key : concurrentDocument.getFrequency_table().keySet()) {
                synchronized (locks.get(key)){
                    terms_count_all_docs[key] += 1;
                }
            }
        });
        return terms_count_all_docs;
    }
    public static Duration process(List<ConcurrentDocument> documentList, double[] terms_count_all_docs) throws IOException {
        Optional<Duration> doc_avgduration = Optional.of(Duration.ZERO);
        Schema schema_tfidf = new Schema.Parser().parse(new FileInputStream(tfidf_schema_path));
        int doc_len = documentList.size();
        //
        BlockingQueue<GenericData.Record> buffer = new LinkedBlockingQueue<>(doc_len);

        GenericData.Record record = new GenericData.Record(schema_tfidf);
        AtomicLong n = new AtomicLong(0);
        doc_avgduration = documentList.parallelStream().map(concurrentDocument -> {
            Instant doc_start = Instant.now();
            record.put("doc", concurrentDocument.getTitle());
            try {
                processDocument(terms_count_all_docs, doc_len, record, concurrentDocument, buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return Duration.between(doc_start, Instant.now());
        }).reduce((duration, duration2) ->
                duration.plus(duration2.minus(duration).
                        dividedBy(n.incrementAndGet())));
        if (doc_avgduration.isPresent())
            return doc_avgduration.get();
        else {
            throw new RuntimeException();
        }
    }
    private static void processDocument(double[] terms_count_all_docs,
                                        int doc_len,
                                        GenericData.Record record_orig,
                                        ConcurrentDocument doc,
                                        BlockingQueue<GenericData.Record> buffer) throws IOException {
        for (Integer key: doc.getFrequency_table().keySet()) {
            double idf = Math.log(doc_len / terms_count_all_docs[key]);
            double tf = doc.calculateTermFrequency(key);
            GenericData.Record record = new GenericData.Record(record_orig, false);
            record.put("term", ConcurrentDocument.id_token_vocabulary.get(key));
            record.put("value", tf*idf);
            try {
                buffer.put(record);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
