package jv.microbenchmark.runners;

import jv.microbenchmark.ExecutionPlan;
import jv.records.Document;
import jv.records.Data;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class BasicSerialRunner {
    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        Map<String, Long> count = new HashMap<>();
        long n_docs = 0L;
        try(Stream<String> lines = Files.lines(plan.input_path)) {
            List<String> stringList = lines.toList();
            n_docs = stringList.size();
            for (String line: stringList) {
                for (String term: plan.util.setOfTerms(line, plan.stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
        } catch (IOException e) {
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
            for (String line: lines.toList()) {
                Document doc = plan.util.createDocument(line, plan.stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(plan.n_docs.get() / (double) plan.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    blackhole.consume(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
