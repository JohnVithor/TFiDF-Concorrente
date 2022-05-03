package jv.microbenchmark.runners.tfidf;

import jv.MyBuffer;
import jv.microbenchmark.ExecutionPlan;
import jv.records.Data;
import jv.records.Document;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadConcurrentRunner {
    @Benchmark
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        Map<String, Long> count = new HashMap<>();
        int n_docs = 0;
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Long>> counts = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(1000);
        String endLine = "__END__";
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); ++i) {
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
        try(BufferedReader reader = Files.newBufferedReader(plan.input_path)) {
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
        blackhole.consume(count);
        blackhole.consume(n_docs);
    }
    @Benchmark
    public void compute_tfidf(ExecutionPlan plan, Blackhole blackhole) {
        List<Thread> threads = new ArrayList<>();
        BlockingQueue<String> buffer = new LinkedBlockingQueue<>(1000);
        String endLine = "__END__";
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); ++i) {
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        Document doc = plan.util.createDocument(line, plan.stopwords);
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(plan.n_docs.get() / (double) plan.count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);
                            blackhole.consume(data);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        try(BufferedReader reader = Files.newBufferedReader(plan.input_path)) {
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
    }
}
