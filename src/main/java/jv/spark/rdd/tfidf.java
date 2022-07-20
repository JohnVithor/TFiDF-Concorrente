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

    public tfidf(Set<String> stopworlds, String corpus_path) {
        this.stopwords = stopworlds;
        this.corpus_path = corpus_path;
        final SparkConf sparkConf = new SparkConf().
                setAppName("TFiDF").
                setMaster("local[*]");
        spark = new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
         Set<String> stopwords = util.load_stop_words("stopwords.txt");
        String corpus_path = "datasets/devel.csv";
        TFiDFInterface tfidf = new tfidf(stopwords, corpus_path);
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    public static class TupleComparator implements Comparator<Tuple2<String,Long>>, Serializable {
        @Override
        public int compare(Tuple2<String,Long> x, Tuple2<String,Long> y) {
            return Long.compare(x._2(), y._2());
        }
    }

    public static class DataComparator implements Comparator<Data>, Serializable {
        @Override
        public int compare(Data x, Data y) {
            return Double.compare(x.value(), y.value());
        }
    }

    @Override
    public void compute() {
        final JavaRDD<String> lines = spark.textFile(corpus_path);
        n_docs = lines.count();

        final JavaRDD<String> setOfTerms = lines.flatMap(new SetOfTermsFunctor(stopwords));
        final JavaPairRDD<String, Long> ones = setOfTerms.mapToPair(s -> new Tuple2<>(s, 1L));
        count = setOfTerms.countByValue();
        final JavaPairRDD<String, Long> counts = ones.reduceByKey(Long::sum);
        Tuple2<String,Long> r = counts.max(new TupleComparator());
        most_frequent_term_count = r._2();
        most_frequent_terms = Collections.singletonList(r._1());

        final JavaRDD<Data> datas = lines.flatMap(new EvalDocumentFunctor(stopwords, count, n_docs));
        Data max = datas.max(new DataComparator());
        Data min = datas.min(new DataComparator());
        highest_tfidf = Collections.singletonList(max);
        lowest_tfidf = Collections.singletonList(min);
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
