package main.naive;

import main.Data;
import main.MyWriter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {}
        }));
        String filename = args[0];
        Path input_path = Path.of("datasets/" + filename + ".csv");
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String tfidf_out_fileName = "concurrent_naive/" + filename + "_tfidf_results.parquet";
        String log_output = "concurrent_naive/output_" + filename + ".log";
        PrintStream log = new PrintStream(new OutputStream() {
            final FileOutputStream f = new FileOutputStream(log_output);
            @Override
            public void write(int b) throws IOException {
                f.write(b);
                System.out.write(b);
            }
        });
        Instant start = Instant.now();
        Set<String> stopwords = Utils.load_stop_words(stop_words_path);
        int n_docs = 0;
        Map<String, Long> count = new HashMap<>();
        try(Stream<String> lines = Files.lines(input_path)) {
            List<String> stringList = lines.toList();
            n_docs = stringList.size();
            List<Thread> threads = new ArrayList<>();
            List<Map<String, Long>> counts = new ArrayList<>();
            int docs_per_thread = n_docs / Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < Runtime.getRuntime().availableProcessors()-1; ++i) {
                Map<String, Long> count_i = new HashMap<>();
                int finalI = i;
                Thread t = new Thread(() -> {
                    for (int j = finalI*docs_per_thread; j < (finalI+1)*docs_per_thread; j++) {
                        for (String term: Utils.setOfTerms(stringList.get(j), stopwords)) {
                            count_i.put(term, count_i.getOrDefault(term, 0L)+1L);
                        }
                    }
                });
                t.start();
                threads.add(t);
                counts.add(count_i);
            }
            for (int j = 3*docs_per_thread; j < n_docs; j++) {
                for (String term: Utils.setOfTerms(stringList.get(j), stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
            for (Thread t : threads) {
                t.join();
            }
            for (Map<String, Long> c : counts) {
                for (Map.Entry<String, Long> pair : c.entrySet()) {
                    count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Instant mid = Instant.now();

        log.println(Duration.between(start, mid).toMillis());
        log.println(Duration.between(start, mid).toSeconds());
        log.println(Duration.between(start, mid).toMinutes());

        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        try(Stream<String> lines = Files.lines(input_path)) {
            for (String line: lines.toList()) {
                Utils.Document doc = Utils.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    try {
                        myWriter.write(data);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
        Instant end = Instant.now();

        log.println(Duration.between(mid, end).toMillis());
        log.println(Duration.between(mid, end).toSeconds());
        log.println(Duration.between(mid, end).toMinutes());

        log.println(Duration.between(start, end).toMillis());
        log.println(Duration.between(start, end).toSeconds());
        log.println(Duration.between(start, end).toMinutes());
    }
}
