package main;

import org.apache.avro.Schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String filename = "test_id";
//    static private final String filename = "devel_100_000_id";
    static private final String input_path = "datasets/"+filename+".csv";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String tfidf_out_fileName = "results_concurrent/" + filename+ "_tfidf_results.parquet";
    static private final String log_output = "logs_concurrent/output_"+filename+".log";

    public static void main(String[] args) throws IOException, InterruptedException {
        Instant start = Instant.now();
        Set<String> stopwords = Utils.load_stop_words(stop_words_path);

        List<Utils.Document> docs = getDocumentList(stopwords);

        Instant p1 = Instant.now();
        System.out.println(Duration.between(start, p1).toMillis());

        Map<String, Long> count = Utils.computeTermDocFreq(docs);

        Instant p2 = Instant.now();
        System.out.println(Duration.between(p1, p2).toMillis());

//        computeTFiDFProducerConsumer(docs, count);
        Serial.computeTFiDFWriting(docs, count);

        Instant p3 = Instant.now();
        System.out.println(Duration.between(p2, p3).toMillis());
    }

    public static List<Utils.Document> getDocumentList(Set<String> stopwords) {
        List<Utils.Document> docs;
        try(Stream<String> lines = Files.lines(Path.of(input_path))) {
            docs = lines
                    .parallel()
                    .map(line -> Utils.createDocument(line, stopwords)).toList();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return docs;
    }

    public static void computeTFiDFProducerConsumer(List<Utils.Document> docs, Map<String, Long> count) throws IOException, InterruptedException {
        int n_docs = docs.size();

        Data end = new Data(null,0,0.0);
        ConcurrentLinkedQueue<Data> buffer = new ConcurrentLinkedQueue<>();

        RecordConsumer recordConsumer = new RecordConsumer(
                tfidf_out_fileName,
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)),
                end,
                buffer
        );
        Thread consumer = new Thread(recordConsumer);
        consumer.start();

        docs.stream().parallel().forEach(doc -> {
            for (String key: doc.counts().keySet()) {
                double idf = Math.log( n_docs / (double) count.get(key));
                double tf = doc.counts().get(key) / (double) doc.n_terms();
                Data data = new Data(key, doc.id(), tf*idf);
                buffer.add(data);
            }
        });
        buffer.add(end);
        consumer.join();
    }
}
