package jv.microbenchmark.runner.executor;

import jv.microbenchmark.TFiDFExecutionPlan;
import jv.records.Data;
import jv.tfidf.executor.TaskDF;
import jv.tfidf.executor.TaskTFiDF;
import jv.tfidf.naive.threads.Compute_DF_ConsumerThread;
import jv.tfidf.naive.threads.Compute_TFiDF_ConsumerThread;
import jv.utils.MyBuffer;
import org.apache.commons.lang3.tuple.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ExecutorConcurrentRunner {
    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        Long most_frequent_term_count = 0L;
        List<String> most_frequent_terms = new ArrayList<>();
        Map<String, Long> count = new HashMap<>();
        long n_docs = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(plan.n_threads);
        final List<Future<HashMap<String, Long>>> counts = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                TaskDF task = new TaskDF(line, plan.util, plan.stopwords);
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
        most_frequent_term_count = plan.util.compute_mft(
                count, most_frequent_term_count, most_frequent_terms
        );
        blackhole.consume(count);
        blackhole.consume(n_docs);
        blackhole.consume(most_frequent_term_count);
        blackhole.consume(most_frequent_terms);
    }

    @Benchmark
    public void compute_tfidf(TFiDFExecutionPlan plan, Blackhole blackhole) {
        ExecutorService executorService = Executors.newFixedThreadPool(plan.n_threads);
        List<Data> highest_tfidf = new ArrayList<>();
        List<Data> lowest_tfidf = new ArrayList<>();
        final List<Future<Pair<ArrayList<Data>, ArrayList<Data>>>> dataPairs = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                TaskTFiDF task = new TaskTFiDF(line, plan.n_docs, plan.util, plan.stopwords, plan.count);
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
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
