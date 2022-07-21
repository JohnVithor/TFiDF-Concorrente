package jv.spark.dataset;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.spark.rdd.EvalDocumentFunctor;
import jv.spark.rdd.SetOfTermsFunctor;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.stream.collectors.MaxTermCount;
import jv.tfidf.stream.collectors.MaxTermCountCollector;
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
import java.time.Duration;
import java.time.Instant;
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
                .master("local[*]")
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
        final Dataset<Row> df = spark.read().format("csv")
                .option("delimiter", ";").option("header", false)
                .schema(schema).load(corpus_path);
        df.createOrReplaceTempView("data");

        final Dataset<Row> df2 = spark.sql("SELECT id, CONCAT(title, ' ', content) AS content FROM data");
        df2.createOrReplaceTempView("data");
        n_docs = df2.count();

        spark.udf().register("setOfTerms", new SetOfTermsUDF(stopwords), DataTypes.createArrayType(DataTypes.StringType));
        final Dataset<Row> setOfTerms = df2.withColumn("terms", callUDF("setOfTerms", col("content")));
        setOfTerms.createOrReplaceTempView("data");
        final Dataset<Row> termsCount = spark.sql("SELECT id, explode(terms) AS term from data")
                .groupBy(col("id"), col("term")).
                count().orderBy(col("id").asc(),col("term").asc());
        termsCount.createOrReplaceTempView("termsCount");
        count = new HashMap<>();
        termsCount.collectAsList().forEach(row -> count.put(row.getString(1), row.getLong(2)));

        spark.udf().register("evalDocs", new EvalDocumentUDF(stopwords, count, n_docs), DataTypes.createMapType(DataTypes.StringType,DataTypes.DoubleType));
        final Dataset<Row> datas = df2.withColumn("datas", callUDF("evalDocs", col("content")));
        datas.createOrReplaceTempView("data");
        final Dataset<Row> datasraw = spark.sql("SELECT id, datas from data");
        datasraw.show(5);

    }

    @Override
    public void compute_df() {

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
