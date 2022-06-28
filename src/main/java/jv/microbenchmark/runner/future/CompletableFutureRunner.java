package jv.microbenchmark.runner.future;

import jv.microbenchmark.TFiDFExecutionPlan;
import jv.records.Data;
import jv.tfidf.future.threads.ConsumerThreadDF;
import jv.tfidf.naive.threads.ConsumerThreadTFiDF;
import jv.utils.MyBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CompletableFutureRunner {
    private final static String endLine = "__END__";

    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        Long most_frequent_term_count = 0L;
        List<String> most_frequent_terms = new ArrayList<>();
        Map<String, Long> count = new HashMap<>();
        int n_docs = 0;
        final List<CompletableFuture<HashMap<String, Long>>> futures = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            CompletableFuture<HashMap<String, Long>> f = new CompletableFuture<>();
            ConsumerThreadDF t = new ConsumerThreadDF(
                    buffer, plan.util, plan.stopwords, endLine, f
            );
            t.start();
            futures.add(f);
        }
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (int i = 0; i < plan.n_threads; ++i) {
                buffer.put(endLine);
            }
            for (CompletableFuture<HashMap<String, Long>> f : futures) {
                HashMap<String, Long> t = f.get();
                for (Map.Entry<String, Long> pair : t.entrySet()) {
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
        List<Data> highest_tfidf = new ArrayList<>();
        List<Data> lowest_tfidf = new ArrayList<>();
        final List<ConsumerThreadTFiDF> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            ConsumerThreadTFiDF t = new ConsumerThreadTFiDF(
                    buffer, plan.util, plan.stopwords, endLine, plan.count, plan.n_docs
            );
            t.start();
            threads.add(t);
        }
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (int i = 0; i < plan.n_threads; ++i) {
                buffer.put(endLine);
            }
            double htfidf_final = 0.0;
            double ltfidf_final = Double.MAX_VALUE;
            for (ConsumerThreadTFiDF t : threads) {
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
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
