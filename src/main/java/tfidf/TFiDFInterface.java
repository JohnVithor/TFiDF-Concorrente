package tfidf;

import records.TFiDFInfo;

public interface TFiDFInterface {

    void compute_df();

    void compute_tfidf();

    default void compute() {
//        Instant start = Instant.now();
        compute_df();
//        Instant mid = Instant.now();
        compute_tfidf();
//        Instant end = Instant.now();

//        System.out.println(Duration.between(start, mid).toMillis());
//        System.out.println(Duration.between(mid, end).toMillis());
//        System.out.println(Duration.between(start, end).toMillis());
    }

    TFiDFInfo results();
}
