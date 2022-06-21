package jv.microbenchmark.runner.forkjoin;

import jv.microbenchmark.TFiDFExecutionPlan;
import jv.records.Data;
import jv.tfidf.forkjoin.JoinHashTask;
import jv.tfidf.forkjoin.JoinTFiDFTask;
import jv.tfidf.naive.threads.ConsumerThreadDF;
import jv.tfidf.naive.threads.ConsumerThreadTFiDF;
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
import java.util.concurrent.ForkJoinPool;

public class ForkJoinRunner {
    private final static String endLine = "__END__";

    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        Long most_frequent_term_count = 0L;
        List<String> most_frequent_terms = new ArrayList<>();
        Map<String, Long> count;
        int n_docs = 0;
        final List<ConsumerThreadDF> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            ConsumerThreadDF t = new ConsumerThreadDF(
                    buffer, plan.util, plan.stopwords, endLine
            );
            t.start();
            threads.add(t);
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
            JoinHashTask join = new JoinHashTask(threads);
            ForkJoinPool pool = ForkJoinPool.commonPool();
            count = pool.invoke(join);
            pool.shutdown();
        } catch (InterruptedException e) {
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
        List<Data> highest_tfidf;
        List<Data> lowest_tfidf;
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
            JoinTFiDFTask join = new JoinTFiDFTask(threads);
            ForkJoinPool pool = ForkJoinPool.commonPool();
            Pair<List<Data>, List<Data>> pair = pool.invoke(join);
            highest_tfidf = pair.getKey();
            lowest_tfidf = pair.getValue();
            pool.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
