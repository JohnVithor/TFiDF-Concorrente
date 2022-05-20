package jv.microbenchmark.runners.tfidf.naive;

import jv.microbenchmark.TFiDFExecutionPlan;
import jv.records.Data;
import jv.records.Document;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NaiveSerialRunner {
    @Benchmark
    public void compute_df(TFiDFExecutionPlan plan, Blackhole blackhole) {
        Long most_frequent_term_count = 0L;
        List<String> most_frequent_terms = new ArrayList<>();
        Map<String, Long> count = new HashMap<>();
        long n_docs = 0L;
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                for (String term : plan.util.setOfTerms(line, plan.stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L) + 1L);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(count);
        blackhole.consume(n_docs);
        blackhole.consume(most_frequent_term_count);
        blackhole.consume(most_frequent_terms);
    }

    @Benchmark
    public void compute_tfidf(TFiDFExecutionPlan plan, Blackhole blackhole) {
        List<Data> highest_tfidf = new ArrayList<>();
        List<Data> lowest_tfidf = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(plan.corpus_path)) {
            double htfidf = 0.0;
            double ltfidf = Double.MAX_VALUE;
            String line;
            while ((line = reader.readLine()) != null) {
                Document doc = plan.util.createDocument(line, plan.stopwords);
                for (String key : doc.counts().keySet()) {
                    double idf = Math.log(plan.n_docs / (double) plan.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf * idf);
                    if (data.value() > htfidf) {
                        htfidf = data.value();
                        highest_tfidf.clear();
                        highest_tfidf.add(data);
                    } else if (data.value() == htfidf) {
                        highest_tfidf.add(data);
                    }
                    if (data.value() < ltfidf) {
                        ltfidf = data.value();
                        lowest_tfidf.clear();
                        lowest_tfidf.add(data);
                    } else if (data.value() == ltfidf) {
                        lowest_tfidf.add(data);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        blackhole.consume(highest_tfidf);
        blackhole.consume(lowest_tfidf);
    }
}
