package jv.spark.dataset;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.spark.rdd.EvalDocumentFunctor;
import jv.spark.rdd.SetOfTermsFunctor;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

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
    private final SparkSession spark;
    private final StructType schema;

    public tfidf(Set<String> stopworlds, String corpus_path) {
        this.stopwords = stopworlds;
        this.corpus_path = corpus_path;
        spark = SparkSession.builder()
                .appName("TFiDF")
                .master("local")
                .getOrCreate();
        schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "id",DataTypes.ShortType,false
                ),
                DataTypes.createStructField(
                        "title",DataTypes.StringType,false
                ),
                DataTypes.createStructField(
                        "content",DataTypes.StringType,false
                )
        });
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        String corpus_path = "datasets/10.csv";
        TFiDFInterface tfidf = new tfidf(stopwords, corpus_path);
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {

        final Dataset<Row> df = spark.read().format("csv")
                .option("delimiter", ",").option("header", false)
                .schema(schema).load(corpus_path);
        df.createOrReplaceTempView("data");
        df.printSchema();

        final Dataset<Row> df2 = spark.sql("SELECT CONCAT(title, ' ', content) AS content FROM data");
        df2.createOrReplaceTempView("data");

        n_docs = df2.count();
        df2.show(5);
        spark.udf().register("setOfTerms", new SetOfTermsUDF(stopwords), DataTypes.createArrayType(DataTypes.StringType));

        final Dataset<Row> setOfTerms = df2.withColumn("terms", callUDF("setOfTerms", col("content")));
        setOfTerms.show(5);
        setOfTerms.createOrReplaceTempView("data");

        final Dataset<Row> setOfTermsLen = setOfTerms.withColumn("terms_len",size(col("terms")));
        setOfTermsLen.createOrReplaceTempView("data");
        setOfTermsLen.show(5);

        final Dataset<Row> termsCount = spark.sql("SELECT explode(terms) AS term from data")
                .groupBy(col("term")).count().orderBy(col("count").desc());
        termsCount.createOrReplaceTempView("termsCount");
        termsCount.show(5);

//        final JavaPairRDD<String, Long> ones = setOfTerms.mapToPair(s -> new Tuple2<>(s, 1L));
//        count = setOfTerms.countByValue();
//        final JavaPairRDD<String, Long> counts = ones.reduceByKey(Long::sum);
//        Tuple2<String,Long> r = counts.max(new TupleComparator());
//        most_frequent_term_count = r._2();
//        most_frequent_terms = Collections.singletonList(r._1());
    }

     @Override
    public void compute_tfidf() {

//        final JavaRDD<String> lines = spark.textFile(corpus_path);
//        final JavaRDD<Data> datas = lines.flatMap(new EvalDocumentFunctor(stopwords, count, n_docs));
//        Data max = datas.max(new DataComparator());
//        Data min = datas.min(new DataComparator());
//        highest_tfidf = Collections.singletonList(max);
//        lowest_tfidf = Collections.singletonList(min);
    }

    @Override
    public TFiDFInfo results() {
        return new TFiDFInfo(
//                this.count.size(),
                0,
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
