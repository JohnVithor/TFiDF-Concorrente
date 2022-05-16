package jv.microbenchmark.runner;

import jv.microbenchmark.ExecutionPlan;
import jv.records.Data;
import jv.tfidf.stream.collectors.MaxTermCount;
import jv.tfidf.stream.collectors.MaxTermCountCollector;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDF;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDFCollector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@State(Scope.Thread)
public class StreamSerialRunner {

    private Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;
    private final Object lock = new Object();
    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private List<Data> highest_tfidf = new ArrayList<>();
    private List<Data> lowest_tfidf = new ArrayList<>();
    @Benchmark
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        try (Stream<String> lines = Files.lines(plan.corpus_path)) {
            count = lines
                    .sequential()
                    .peek(s -> {
                        synchronized (lock) {
                            ++n_docs;
                        }
                    })
                    .flatMap(line -> plan.util.setOfTerms(line, plan.stopwords).stream())
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MaxTermCount r = this.count.entrySet()
                .stream().sequential().collect(new MaxTermCountCollector());
        most_frequent_term_count = r.getMax_count();
        most_frequent_terms = r.getTerms().stream().sequential().sorted().toList();
        blackhole.consume(count);
        blackhole.consume(n_docs);
        blackhole.consume(most_frequent_term_count);
        blackhole.consume(most_frequent_terms);
    }
    @Benchmark
    public void compute_tfidf(ExecutionPlan plan, Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(plan.corpus_path)) {
            MinMaxTermsTFiDF r = lines
                    .sequential()
                    .map(line -> plan.util.createDocument(line, plan.stopwords))
                    .flatMap(doc -> doc.counts().entrySet().stream().map(e -> {
                        double idf = Math.log(n_docs / (double) count.get(e.getKey()));
                        double tf = e.getValue() / (double) doc.n_terms();
                        return new Data(e.getKey(), doc.id(), tf*idf);
                    }))
                    .collect(new MinMaxTermsTFiDFCollector());
            this.highest_tfidf = r.getHighest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
            this.lowest_tfidf = r.getLowest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
