package jv.microbenchmark.runners.tfidf;

import jv.microbenchmark.ExecutionPlan;
import jv.records.Document;
import jv.records.Data;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class BasicSerialRunner {
    @Benchmark
    public void compute_df(ExecutionPlan plan, Blackhole blackhole) {
        Map<String, Long> count = new HashMap<>();
        long n_docs = 0L;
        try(BufferedReader reader = Files.newBufferedReader(plan.input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
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
    @Benchmark
    public void compute_tfidf(ExecutionPlan plan, Blackhole blackhole) {
        try(BufferedReader reader = Files.newBufferedReader(plan.input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
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
