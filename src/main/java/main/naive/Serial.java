package main.naive;

import main.Data;
import main.ExecutionData;
import main.MyWriter;
import main.TFiDF;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Serial implements TFiDF {
    static private final String stop_words_path = "datasets/stopwords.txt";
    private Path input_path;
    private String tfidf_schema_path;
    private String tfidf_out_fileName;
    private Set<String> stopwords;
    private final Map<String, Long> count = new HashMap<>();
    int n_docs = 0;
    public void setup(String target) {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {}
        }));
        input_path = Path.of("datasets/" + target + ".csv");
        tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        tfidf_out_fileName = "serial_naive/" + target + "_tfidf_results.parquet";
        stopwords = Utils.load_stop_words(stop_words_path);
    }

    @Override
    public void firstHalf() {
        try(Stream<String> lines = Files.lines(input_path)) {
            List<String> stringList = lines.toList();
            n_docs = stringList.size();
            for (String line: stringList) {
                for (String term: Utils.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void secondHalfWriting() throws IOException {
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        try(Stream<String> lines = Files.lines(input_path)) {
            for (String line: lines.toList()) {
                Utils.Document doc = Utils.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    try {
                        myWriter.write(data);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
    }

    @Override
    public void secondHalfNotWriting() {
        try(Stream<String> lines = Files.lines(input_path)) {
            for (String line: lines.toList()) {
                Utils.Document doc = Utils.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
