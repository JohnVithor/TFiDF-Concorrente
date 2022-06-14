package jv.tfidf.executor;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.executor.callable.TaskLowerHigherTFiDF;
import jv.tfidf.executor.callable.TaskSetOfTerms;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class SmallTasksConcurrent implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final int n_threads;
    private final Map<String, Long> count = new HashMap<>();
    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();
    private long n_docs = 0L;
    private Long most_frequent_term_count = 0L;

    public SmallTasksConcurrent(Set<String> stopworlds, UtilInterface util,
                                Path corpus_path, int n_threads) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.n_threads = n_threads;
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        Path corpus_path = Path.of("datasets/test.csv");
        TFiDFInterface tfidf = new SmallTasksConcurrent(
                stopwords, util, corpus_path, 4
        );
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        ExecutorService executorService =
                new ThreadPoolExecutor(n_threads, n_threads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(10));
        final List<Future<HashMap<String, Long>>> counts = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(corpus_path)) {
            String line;
            // causando problemas de memória!
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                TaskSetOfTerms task = new TaskSetOfTerms(line, util, stopwords);
                counts.add(executorService.submit(task));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            executorService.shutdown();
            for (Future<HashMap<String, Long>> c : counts) {
                for (Map.Entry<String, Long> pair : c.get().entrySet()) {
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
        try (BufferedReader reader = Files.newBufferedReader(corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                TaskLowerHigherTFiDF task = new TaskLowerHigherTFiDF(line, n_docs, util, stopwords, count);
                dataPairs.add(executorService.submit(task));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            double htfidf_final = 0.0;
            double ltfidf_final = Double.MAX_VALUE;
            executorService.shutdown();
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
        this.highest_tfidf.sort(Comparator.comparingDouble(Data::doc_id));
        this.lowest_tfidf.sort(Comparator.comparingDouble(Data::doc_id));
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
