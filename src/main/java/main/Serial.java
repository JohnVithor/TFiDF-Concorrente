package main;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

public class Serial {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private String filename = "test_id";
//    static private final String filename = "devel_100_000_id";
    static private String input_path = "datasets/"+filename+".csv";
    static private String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private String tfidf_out_fileName = "results_serial/" + filename+ "_tfidf_results.parquet";
    static private String log_output = "logs_serial/output_"+filename+".log";

    public static void main(String[] args) throws IOException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {

            }
        }));
        filename = args[0];
        input_path = "datasets/"+filename+".csv";
        tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        tfidf_out_fileName = "results_serial/" + filename+ "_tfidf_results.parquet";
        log_output = "logs_serial/output_"+filename+".log";
        Instant start = Instant.now();
        Set<String> stopwords = Utils.load_stop_words(stop_words_path);

        List<Utils.Document> docs = getSerialDocumentList(stopwords);

        Instant p1 = Instant.now();
        System.out.println(Duration.between(start, p1).toMillis());

        Map<String, Long> count = Utils.computeTermDocFreq(docs);

        Instant p2 = Instant.now();
        System.out.println(Duration.between(p1, p2).toMillis());

        computeTFiDFWriting(docs, count);

        Instant end = Instant.now();
        System.out.println(Duration.between(p2, end).toMillis());
    }

    public static List<Utils.Document> getSerialDocumentList(Set<String> stopwords) {
        List<Utils.Document> docs = new ArrayList<>();
        try(Stream<String> lines = Files.lines(Path.of(input_path))) {
            docs = lines
                    .sequential()
                    .map(line -> Utils.createDocument(line, stopwords))
                    .toList();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return docs;
    }

    public static void computeTFiDFWriting(List<Utils.Document> docs, Map<String, Long> count) throws IOException {
        int n_docs = docs.size();

        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        for (Utils.Document doc:docs) {
            for (String key: doc.counts().keySet()) {
                double idf = Math.log( n_docs / (double) count.get(key));
                double tf = doc.counts().get(key) / (double) doc.n_terms();
                Data data = new Data(key, doc.id(), tf*idf);
                try {
                    myWriter.write(data);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        myWriter.close();
    }
}
