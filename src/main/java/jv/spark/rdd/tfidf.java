package jv.spark.rdd;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class tfidf implements TFiDFInterface {
    private final Set<String> stopwords;
    private final String corpus_path;
    private Map<String, Long> count = new HashMap<>();
    private long n_docs;
    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private List<Data> highest_tfidf = new ArrayList<>();
    private List<Data> lowest_tfidf = new ArrayList<>();
    private final JavaSparkContext spark;

    public tfidf(Set<String> stopwords, String corpus_path) {
        this.stopwords = stopwords;
        this.corpus_path = corpus_path;
        final SparkConf sparkConf = new SparkConf().
                setAppName("TFiDF").
                setMaster("local[*]");
        spark = new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) throws IOException {
        Instant start = Instant.now();
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        String corpus_path = "datasets/train.csv";
        TFiDFInterface tfidf = new tfidf(stopwords, corpus_path);
        tfidf.compute();
        System.out.println(tfidf.results());
        Instant end = Instant.now();
        System.err.println(Duration.between(start, end).toMillis());
    }

    @Override
    public void compute() {
        final JavaRDD<String> lines = spark.textFile(corpus_path);
        n_docs = lines.count();
        final JavaRDD<String> setOfTerms = lines.flatMap(new SetOfTermsFunctor(stopwords));
        count = setOfTerms.countByValue();
        final JavaRDD<Data> datas = lines.flatMap(new EvalDocumentFunctor(stopwords, count, n_docs));
        System.out.println(datas.take(5));
    }
    @Override
    public void compute_df() {
    }

    @Override
    public void compute_tfidf() {
    }

    @Override
    public TFiDFInfo results() {
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
