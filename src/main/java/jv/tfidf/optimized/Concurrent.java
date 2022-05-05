package jv.tfidf.optimized;

import jv.records.Data;
import jv.utils.MyWriter;
import jv.utils.StreamApacheUtil;
import jv.utils.UtilInterface;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";
    public static void run(String target) throws IOException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {}
        }));
        Path input_path = Path.of("datasets/" + target + ".csv");
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String tfidf_out_fileName = "concurrent_optimized/" + target + "_tfidf_results.parquet";
        UtilInterface util = new StreamApacheUtil();
        Set<String> stopwords = util.load_stop_words(stop_words_path);
        AtomicInteger n_docs = new AtomicInteger();
        Map<String, Long> count;
        Files.lines(input_path).count();
        try(Stream<String> lines = Files.lines(input_path)) {
            count = lines
                    .parallel()
                    .peek(e -> n_docs.getAndIncrement())
                    .map(line -> util.setOfTerms(line, stopwords))
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(token -> token,
                             Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));
        try(Stream<String> lines = Files.lines(input_path)) {
            lines
//                    .parallel()
                    .sequential()
                    .map(line -> util.createDocument(line, stopwords))
                    .forEach(doc -> {
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(n_docs.get() / (double) count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);
                            try {
                                myWriter.write(data);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
    }
}
