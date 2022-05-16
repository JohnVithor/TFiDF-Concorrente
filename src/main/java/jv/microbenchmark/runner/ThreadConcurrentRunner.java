package jv.microbenchmark.runner;

import jv.utils.MyBuffer;
import jv.microbenchmark.ExecutionPlan;
import jv.records.Data;
import jv.records.Document;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

@State(Scope.Thread)
public class ThreadConcurrentRunner {
    static private final String endLine = "__END__";
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();

    @Benchmark
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Long>> counts = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        for (int i = 0; i < plan.n_threads; ++i) {
            Map<String, Long> count_i = new HashMap<>();
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        for (String term: plan.util.setOfTerms(line, plan.stopwords)) {
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
        try(BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
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
        for (Map.Entry<String, Long> entry: this.count.entrySet()) {
            if (entry.getValue() > most_frequent_term_count) {
                most_frequent_term_count = entry.getValue();
                most_frequent_terms.clear();
                most_frequent_terms.add(entry.getKey());
            } else if (entry.getValue().equals(most_frequent_term_count)) {
                most_frequent_terms.add(entry.getKey());
            }
        }
        blackhole.consume(count);
        blackhole.consume(n_docs);
        blackhole.consume(most_frequent_term_count);
        blackhole.consume(most_frequent_terms);
    }
    @Benchmark
    public void compute_tfidf(ExecutionPlan plan, Blackhole blackhole) {
        final ArrayList<ArrayList<Data>> data_high = new ArrayList<>();
        final ArrayList<ArrayList<Data>> data_low = new ArrayList<>();
        final double[] htfidf = new double[plan.n_threads];
        final double[] ltfidf = new double[plan.n_threads];
        for (int i = 0; i < plan.n_threads; i++) {
            data_high.add(new ArrayList<>());
            data_low.add(new ArrayList<>());
            ltfidf[i] = Double.MAX_VALUE;
        }
        List<Thread> threads = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(plan.buffer_size);
        long finalN_docs = plan.n_docs;
        for (int i = 0; i < plan.n_threads; ++i) {
            int finalI = i;
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        Document doc = plan.util.createDocument(line, plan.stopwords);
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(finalN_docs / (double) plan.count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);

                            if (data.value() > htfidf[finalI]) {
                                htfidf[finalI] = data.value();
                                data_high.get(finalI).clear();
                                data_high.get(finalI).add(data);
                            } else if (data.value() == htfidf[finalI]) {
                                data_high.get(finalI).add(data);
                            }
                            if (data.value() < ltfidf[finalI]) {
                                ltfidf[finalI] = data.value();
                                data_low.get(finalI).clear();
                                data_low.get(finalI).add(data);
                            } else if (data.value() == ltfidf[finalI]) {
                                data_low.get(finalI).add(data);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        try(BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
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
        double htfidf_final = 0.0;
        double ltfidf_final = Double.MAX_VALUE;
        for (int i = 0; i < plan.n_threads; ++i) {
            if (htfidf[i] > htfidf_final) {
                htfidf_final = htfidf[i];
                highest_tfidf.clear();
                highest_tfidf.addAll(data_high.get(i));
            } else if (htfidf[i] == htfidf_final) {
                highest_tfidf.addAll(data_high.get(i));
            }
            if (ltfidf[i] < ltfidf_final) {
                ltfidf_final = ltfidf[i];
                lowest_tfidf.clear();
                lowest_tfidf.addAll(data_low.get(i));
            } else if (ltfidf[i] == ltfidf_final) {
                lowest_tfidf.addAll(data_low.get(i));
            }
        }
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
