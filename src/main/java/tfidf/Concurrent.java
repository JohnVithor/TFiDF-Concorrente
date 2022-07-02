package tfidf;

import records.Data;
import records.TFiDFInfo;
import tfidf.threads.Compute_DF_ConsumerThread;
import tfidf.threads.Compute_TFiDF_ConsumerThread;
import utils.ForEachApacheUtil;
import utils.MyBuffer;
import utils.UtilInterface;

import java.io.BufferedReader;
import java.io.IOException;
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
    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();
    private long n_docs = 0L;
    private Long most_frequent_term_count = 0L;

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path, int n_threads, int buffer_size) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.n_threads = n_threads;
        this.buffer_size = buffer_size;
    }

    public static void main(String[] args) {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        Path corpus_path = Path.of("datasets/train.csv");
        TFiDFInterface tfidf = new Concurrent(
                stopwords, util, corpus_path, 12, 1000
        );
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        final List<Compute_DF_ConsumerThread> runnables = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            Compute_DF_ConsumerThread r = new Compute_DF_ConsumerThread(
                    buffer, util, stopwords, endLine
            );
            runnables.add(r);
            threads.add(Thread.ofVirtual().start(r));
        }
        try (BufferedReader reader = Files.newBufferedReader(corpus_path)) {
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
            for (int i = 0; i < n_threads; ++i) {
                threads.get(i).join();
                for (Map.Entry<String, Long> pair : runnables.get(i).getCount().entrySet()) {
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
    public void compute_tfidf() {
        final List<Compute_TFiDF_ConsumerThread> runnables = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            Compute_TFiDF_ConsumerThread r = new Compute_TFiDF_ConsumerThread(
                    buffer, util, stopwords, endLine, count, n_docs
            );
            runnables.add(r);
            threads.add(Thread.ofVirtual().start(r));
        }
        try (BufferedReader reader = Files.newBufferedReader(corpus_path)) {
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
            for (int i = 0; i < n_threads; ++i) {
                threads.get(i).join();
                Compute_TFiDF_ConsumerThread r = runnables.get(i);
                if (r.getHtfidf() > htfidf_final) {
                    htfidf_final = r.getHtfidf();
                    highest_tfidf.clear();
                    highest_tfidf.addAll(r.getData_high());
                } else if (r.getHtfidf() == htfidf_final) {
                    highest_tfidf.addAll(r.getData_high());
                }
                if (r.getLtfidf() < ltfidf_final) {
                    ltfidf_final = r.getLtfidf();
                    lowest_tfidf.clear();
                    lowest_tfidf.addAll(r.getData_low());
                } else if (r.getLtfidf() == ltfidf_final) {
                    lowest_tfidf.addAll(r.getData_low());
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
