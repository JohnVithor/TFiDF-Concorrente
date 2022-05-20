package jv.tfidf.naive;

import jv.records.TFiDFInfo;
import jv.tfidf.naive.threads.Compute_DF_ConsumerThread;
import jv.tfidf.naive.threads.Compute_TFiDF_ConsumerThread;
import jv.utils.MyBuffer;
import jv.records.Data;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Concurrent implements TFiDFInterface {
    static private final String endLine = "__END__";
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final int n_threads;
    private final int buffer_size;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/test.csv");
        TFiDFInterface tfidf = new Concurrent(
                stopwords, util, corpus_path, 4, 1000
        );
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path, int n_threads, int buffer_size) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.n_threads = n_threads;
        this.buffer_size = buffer_size;
    }

    @Override
    public void compute_df() throws IOException {
        final List<Compute_DF_ConsumerThread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            Compute_DF_ConsumerThread t = new Compute_DF_ConsumerThread(
                    buffer, util, stopwords, endLine
            );
            t.start();
            threads.add(t);
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
            for (int i = 0; i < n_threads; ++i) {
                buffer.put(endLine);
            }
            for (Compute_DF_ConsumerThread t : threads) {
                t.join();
                for (Map.Entry<String, Long> pair : t.getCount().entrySet()) {
                    count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        most_frequent_term_count = util.compute_mft(
                count, most_frequent_term_count, most_frequent_terms
        );
    }

    @Override
    public void compute_tfidf() throws IOException {
        final List<Compute_TFiDF_ConsumerThread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            Compute_TFiDF_ConsumerThread t = new Compute_TFiDF_ConsumerThread(
                    buffer, util, stopwords, endLine, count, n_docs
            );
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
            for (int i = 0; i < n_threads; ++i) {
                buffer.put(endLine);
            }
            double htfidf_final = 0.0;
            double ltfidf_final = Double.MAX_VALUE;
            for (Compute_TFiDF_ConsumerThread t : threads) {
                t.join();
                if (t.getHtfidf() > htfidf_final) {
                    htfidf_final = t.getHtfidf();
                    highest_tfidf.clear();
                    highest_tfidf.addAll(t.getData_high());
                } else if (t.getHtfidf() == htfidf_final) {
                    highest_tfidf.addAll(t.getData_high());
                }
                if (t.getLtfidf() < ltfidf_final) {
                    ltfidf_final = t.getLtfidf();
                    lowest_tfidf.clear();
                    lowest_tfidf.addAll(t.getData_low());
                } else if (t.getLtfidf() == ltfidf_final) {
                    lowest_tfidf.addAll(t.getData_low());
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TFiDFInfo results() {
        this.most_frequent_terms.sort(String::compareTo);
        this.highest_tfidf.sort(Comparator.comparingDouble(Data::value));
        this.lowest_tfidf.sort(Comparator.comparingDouble(Data::value));
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
