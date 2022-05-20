package jv.microbenchmark.runner.stream;

import jv.microbenchmark.TFiDFExecutionPlan;
import jv.records.Data;
import jv.tfidf.stream.collectors.MaxTermCount;
import jv.tfidf.stream.collectors.MaxTermCountCollector;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDF;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDFCollector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamSerialRunner {
    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        List<String> most_frequent_terms;
        Long most_frequent_term_count;
        Object lock = new Object();
        final long[] n_docs = {0L};
        Map<String, Long> count;
        try (Stream<String> lines = Files.lines(plan.corpus_path)) {
            count = lines
                    .sequential()
                    .peek(s -> {
                        synchronized (lock) {
                            ++n_docs[0];
                        }
                    })
                    .flatMap(line -> plan.util.setOfTerms(line, plan.stopwords).stream())
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MaxTermCount r = count.entrySet()
                .stream().sequential().collect(new MaxTermCountCollector());
        most_frequent_term_count = r.getMax_count();
        most_frequent_terms = r.getTerms().stream().sequential().sorted().toList();
        blackhole.consume(n_docs[0]);
        blackhole.consume(count);
        blackhole.consume(most_frequent_term_count);
        blackhole.consume(most_frequent_terms);
    }

    @Benchmark
    public void compute_tfidf(TFiDFExecutionPlan plan, Blackhole blackhole) {
        List<Data> highest_tfidf;
        List<Data> lowest_tfidf;
        try (Stream<String> lines = Files.lines(plan.corpus_path)) {
            MinMaxTermsTFiDF r = lines
                    .sequential()
                    .map(line -> plan.util.createDocument(line, plan.stopwords))
                    .flatMap(doc -> doc.counts().entrySet().stream().map(e -> {
                        double idf = Math.log(plan.n_docs / (double) plan.count.get(e.getKey()));
                        double tf = e.getValue() / (double) doc.n_terms();
                        return new Data(e.getKey(), doc.id(), tf * idf);
                    }))
                    .collect(new MinMaxTermsTFiDFCollector());
            highest_tfidf = r.getHighest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
            lowest_tfidf = r.getLowest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
