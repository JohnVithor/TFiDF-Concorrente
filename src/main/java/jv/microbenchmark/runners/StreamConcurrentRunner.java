package jv.microbenchmark.runners;

import jv.microbenchmark.ExecutionPlan;
import jv.records.Data;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamConcurrentRunner {
    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        AtomicInteger n_docs = new AtomicInteger(0);
        Map<String, Long> count;
        try(Stream<String> lines = Files.lines(plan.input_path)) {
            count = lines
                    .parallel()
                    .peek(e -> n_docs.getAndIncrement())
                    .map(line -> plan.util.setOfTerms(line, plan.stopwords))
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
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
            lines
                .parallel()
                .map(line -> plan.util.createDocument(line, plan.stopwords))
                .forEach(doc -> {
                    for (String key: doc.counts().keySet()) {
                        double idf = Math.log(plan.n_docs.get() / (double) plan.count.get(key));
                        double tf = doc.counts().get(key) / (double) doc.n_terms();
                        Data data = new Data(key, doc.id(), tf*idf);
                        blackhole.consume(data);
                    }
                });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
