package jv.tfidf.naive;

import jv.utils.MyBuffer;
import jv.records.Data;
import jv.utils.MyWriter;
import jv.records.Document;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Concurrent implements TFiDFInterface {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String endLine = "__END__";
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final org.apache.hadoop.fs.Path output_path;
    private final int n_threads;
    private final int buffer_size;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_1_000_id.csv");
        org.apache.hadoop.fs.Path output_path =
                new org.apache.hadoop.fs.Path("naive_concurrent/devel_1_000_id_tfidf_results.parquet");
        TFiDFInterface tfidf = new Concurrent(
                stopwords, util, corpus_path, output_path, 4, 1000);
        tfidf.compute();
    }

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path, org.apache.hadoop.fs.Path output_path,
                      int n_threads, int buffer_size) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.output_path = output_path;
        this.n_threads = n_threads;
        this.buffer_size = buffer_size;
    }

    @Override
    public void compute_df() throws IOException {
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Long>> counts = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
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
        try(BufferedReader reader = Files.newBufferedReader(corpus_path)) {
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
    }

    @Override
    public void compute_tfidf() throws IOException {
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(output_path, new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));
        List<Thread> threads = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        long finalN_docs = n_docs;
        for (int i = 0; i < n_threads; ++i) {
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
        try(BufferedReader reader = Files.newBufferedReader(corpus_path)) {
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
