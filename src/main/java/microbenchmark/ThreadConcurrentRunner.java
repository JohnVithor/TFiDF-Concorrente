package microbenchmark;

import records.Data;
import tfidf.threads.DFConsumerRunnable;
import tfidf.threads.TFiDFConsumerRunnable;
import utils.MyBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadConcurrentRunner {
    private final static String endLine = "__END__";

    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        Long most_frequent_term_count = 0L;
        List<String> most_frequent_terms = new ArrayList<>();
        Map<String, Long> count = new HashMap<>();
        int n_docs = 0;
        final List<DFConsumerRunnable> runnables = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            DFConsumerRunnable r = new DFConsumerRunnable(
                    buffer, plan.util, plan.stopwords, endLine
            );
            runnables.add(r);
            threads.add(Thread.ofVirtual().start(r));
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
            for (int i = 0; i < plan.n_threads; ++i) {
                threads.get(i).join();
                for (Map.Entry<String, Long> pair : runnables.get(i).getCount().entrySet()) {
                    count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
            }
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
        List<Data> highest_tfidf = new ArrayList<>();
        List<Data> lowest_tfidf = new ArrayList<>();
        final List<TFiDFConsumerRunnable> runnables = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        final MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            TFiDFConsumerRunnable r = new TFiDFConsumerRunnable(
                    buffer, plan.util, plan.stopwords, endLine, plan.count, plan.n_docs
            );
            runnables.add(r);
            threads.add(Thread.ofVirtual().start(r));
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
            for (int i = 0; i < plan.n_threads; ++i) {
                threads.get(i).join();
                TFiDFConsumerRunnable r = runnables.get(i);
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
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
