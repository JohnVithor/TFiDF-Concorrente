package jv.tfidf.naive;

import jv.MyBuffer;
import jv.records.Data;
import jv.MyWriter;
import jv.records.Document;
import jv.utils.ForEachJavaUtil;
import jv.utils.UtilInterface;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";

    public static void main(String[] args) throws IOException {
        Concurrent.run("devel_100_000_id");
    }
    public static void run(String target) throws IOException {
        Path input_path = Path.of("datasets/" + target + ".csv");
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String tfidf_out_fileName = "concurrent_naive/" + target + "_tfidf_results.parquet";
        UtilInterface util = new ForEachJavaUtil();
        Set<String> stopwords = util.load_stop_words(stop_words_path);
        Map<String, Long> count = new HashMap<>();
        int n_docs = 0;
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Long>> counts = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(1000);
        String endLine = "__END__";
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); ++i) {
            Map<String, Long> count_i = new HashMap<>();
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        for (String term: util.setOfTerms(line, stopwords)) {
                            count_i.put(term, count_i.getOrDefault(term, 0L)+1L);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
            counts.add(count_i);
        }
        try(BufferedReader reader = Files.newBufferedReader(input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (Thread t : threads) {
                buffer.put(endLine);
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (Map<String, Long> c : counts) {
            for (Map.Entry<String, Long> pair : c.entrySet()) {
                count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
            }
        }

        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        threads = new ArrayList<>();
        int finalN_docs = n_docs;
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); ++i) {
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        Document doc = util.createDocument(line, stopwords);
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(finalN_docs / (double) count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);
                            myWriter.write(data);
                        }
                    }
                } catch (InterruptedException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        try(BufferedReader reader = Files.newBufferedReader(input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (Thread t : threads) {
                buffer.put(endLine);
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
    }
}
