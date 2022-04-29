package jv.microbenchmark.runners;

import jv.microbenchmark.ExecutionPlan;
import jv.records.Data;
import jv.records.Document;
import jv.utils.ForEachJavaUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

public class ThreadConcurrentRunner {
    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        Map<String, Long> count = new HashMap<>();
        int n_docs = 0;
        try(Stream<String> lines = Files.lines(plan.input_path)) {
            List<String> stringList = lines.toList();
            n_docs = stringList.size();
            List<Thread> threads = new ArrayList<>();
            List<Map<String, Long>> counts = new ArrayList<>();
            int docs_per_thread = n_docs / Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < Runtime.getRuntime().availableProcessors()-1; ++i) {
                Map<String, Long> count_i = new HashMap<>();
                int finalI = i;
                Thread t = new Thread(() -> {
                    for (int j = finalI*docs_per_thread; j < (finalI+1)*docs_per_thread; j++) {
                        for (String term: plan.util.setOfTerms(stringList.get(j), plan.stopwords)) {
                            count_i.put(term, count_i.getOrDefault(term, 0L)+1L);
                        }
                    }
                });
                t.start();
                threads.add(t);
                counts.add(count_i);
            }
            for (int j = 3*docs_per_thread; j < n_docs; j++) {
                for (String term: plan.util.setOfTerms(stringList.get(j), plan.stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
            for (Thread t : threads) {
                t.join();
            }
            for (Map<String, Long> c : counts) {
                for (Map.Entry<String, Long> pair : c.entrySet()) {
                    count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(count);
        blackhole.consume(n_docs);
    }
    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void compute_tfidf(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.input_path)) {
            List<String> stringList = lines.toList();
            int n_docs = stringList.size();
            List<Thread> threads = new ArrayList<>();
            int docs_per_thread = n_docs / Runtime.getRuntime().availableProcessors();
            for (int i = 0; i < Runtime.getRuntime().availableProcessors()-1; ++i) {
                int finalI = i;
                Thread t = new Thread(() -> {
                    for (int j = finalI*docs_per_thread; j < (finalI+1)*docs_per_thread; j++) {
                        Document doc = plan.util.createDocument(stringList.get(j), plan.stopwords);
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(n_docs / (double) plan.count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);
                            blackhole.consume(data);
                        }
                    }
                });
                t.start();
                threads.add(t);
            }
            for (int j = 3*docs_per_thread; j < n_docs; j++) {
                Document doc = plan.util.createDocument(stringList.get(j), plan.stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) plan.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    blackhole.consume(data);
                }
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
