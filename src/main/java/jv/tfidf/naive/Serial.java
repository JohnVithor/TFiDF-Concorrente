package jv.tfidf.naive;

import jv.records.Data;
import jv.MyWriter;
import jv.records.Document;
import jv.utils.ForEachJavaUtil;
import jv.utils.UtilInterface;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Serial {
    static private final String stop_words_path = "datasets/stopwords.txt";

    public static void main(String[] args) throws IOException {
//        Serial.run("devel_100_000_id");
//        Serial.run("test_id");
        Serial.run("train_id");
    }

    public static void run(String target) throws IOException {
        Instant start = Instant.now();
        String tfidf_out_fileName = "serial_naive/" + target + "_tfidf_results.parquet";
        Path input_path = Path.of("datasets/" + target + ".csv");
        UtilInterface util = new ForEachJavaUtil();
        Set<String> stopwords = util.load_stop_words(stop_words_path);
        Map<String, Long> count = new HashMap<>();
        long n_docs = 0L;
        try(BufferedReader reader = Files.newBufferedReader(input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                for (String term: util.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));
        try(BufferedReader reader = Files.newBufferedReader(input_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Document doc = util.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    myWriter.write(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
        System.out.println(Duration.between(start, Instant.now()).toMillis());
    }
}
