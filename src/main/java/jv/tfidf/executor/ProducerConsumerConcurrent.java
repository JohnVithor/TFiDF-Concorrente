package jv.tfidf.executor;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.executor.callable.TaskDFConsumer;
import jv.tfidf.executor.callable.TaskTFiDFConsumer;
import jv.tfidf.naive.threads.Compute_DF_ConsumerThread;
import jv.tfidf.naive.threads.Compute_TFiDF_ConsumerThread;
import jv.utils.ForEachApacheUtil;
import jv.utils.MyBuffer;
import jv.utils.UtilInterface;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class ProducerConsumerConcurrent implements TFiDFInterface {
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

    public ProducerConsumerConcurrent(Set<String> stopworlds, UtilInterface util,
                                      Path corpus_path, int n_threads, int buffer_size) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.n_threads = n_threads;
        this.buffer_size = buffer_size;
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        Path corpus_path = Path.of("datasets/test.csv");
        TFiDFInterface tfidf = new ProducerConsumerConcurrent(
                stopwords, util, corpus_path, 4, 1000
        );
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        ExecutorService executorService = Executors.newFixedThreadPool(n_threads);
        final List<Future<HashMap<String, Long>>> counts = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            TaskDFConsumer t = new TaskDFConsumer(
                    buffer, util, stopwords, endLine
            );
            counts.add(executorService.submit(t));
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
            executorService.shutdown();
            for (Future<HashMap<String, Long>> f : counts) {
                for (Map.Entry<String, Long> pair : f.get().entrySet()) {
                    count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        most_frequent_term_count = util.compute_mft(
                count, most_frequent_term_count, most_frequent_terms
        );
    }

    @Override
    public void compute_tfidf() {
        ExecutorService executorService = Executors.newFixedThreadPool(n_threads);
        final List<Future<Pair<ArrayList<Data>, ArrayList<Data>>>> dataPairs = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            TaskTFiDFConsumer t = new TaskTFiDFConsumer(
                    buffer, util, stopwords, endLine, count, n_docs
            );
            dataPairs.add(executorService.submit(t));
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
            executorService.shutdown();
            double htfidf_final = 0.0;
            double ltfidf_final = Double.MAX_VALUE;
            for (Future<Pair<ArrayList<Data>, ArrayList<Data>>> f : dataPairs) {
                Pair<ArrayList<Data>, ArrayList<Data>> pair = f.get();
                for (Data t: pair.getKey()) {
                    if (t.value() < ltfidf_final) {
                        ltfidf_final = t.value();
                        lowest_tfidf.clear();
                        lowest_tfidf.add(t);
                    } else if (t.value() == ltfidf_final) {
                        lowest_tfidf.add(t);
                    }
                }
                for (Data t: pair.getValue()) {
                    if (t.value() > htfidf_final) {
                        htfidf_final = t.value();
                        highest_tfidf.clear();
                        highest_tfidf.add(t);
                    } else if (t.value() == htfidf_final) {
                        highest_tfidf.add(t);
                    }
                }
            }
        } catch (InterruptedException | ExecutionException e) {
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
