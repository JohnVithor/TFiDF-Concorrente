package jv.tfidf;

import jv.records.TFiDFInfo;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public interface TFiDFInterface {
    void compute_df() throws IOException;

    void compute_tfidf() throws IOException;

    default void compute() throws IOException {
        Instant start = Instant.now();
        compute_df();
        Instant mid = Instant.now();
        compute_tfidf();
        Instant end = Instant.now();

        System.out.println(Duration.between(start, mid).toMillis());
        System.out.println(Duration.between(mid, end).toMillis());
        System.out.println(Duration.between(start, end).toMillis());
    };

    TFiDFInfo results();
}
