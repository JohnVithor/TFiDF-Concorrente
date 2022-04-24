package main.nonoptimized;

import main.Data;
import main.MyWriter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Serial {
    static private final String stop_words_path = "datasets/stopwords.txt";

    public static void main(String[] args) throws IOException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {

            }
        }));
        String filename = args[0];
        Path input_path = Path.of("datasets/" + filename + ".csv");
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        String tfidf_out_fileName = "serial_nonoptimized/" + filename + "_tfidf_results.parquet";
        String log_output = "serial_nonoptimized/output_"+ filename +".log";
        PrintStream log = new PrintStream(new OutputStream() {
            final FileOutputStream f = new FileOutputStream(log_output);
            @Override
            public void write(int b) throws IOException {
                f.write(b);
                System.out.write(b);
            }
        });
        Instant start = Instant.now();
        Set<String> stopwords = Utils.load_stop_words(stop_words_path);
        AtomicInteger n_docs = new AtomicInteger();
        Map<String, Long> count;
        try(Stream<String> lines = Files.lines(input_path)) {
            count = lines
                .sequential()
                .peek(e -> n_docs.getAndIncrement())
                .map(line -> Utils.setOfTerms(line, stopwords))
                .flatMap(Set::stream).collect(Collectors
                    .groupingBy(token -> token, Collectors.counting()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Instant mid = Instant.now();

        log.println(Duration.between(start, mid).toMillis());
        log.println(Duration.between(start, mid).toSeconds());
        log.println(Duration.between(start, mid).toMinutes());

        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        try(Stream<String> lines = Files.lines(input_path)) {
            lines
                .sequential()
                .map(line -> Utils.createDocument(line, stopwords))
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
        Instant end = Instant.now();

        log.println(Duration.between(mid, end).toMillis());
        log.println(Duration.between(mid, end).toSeconds());
        log.println(Duration.between(mid, end).toMinutes());

        log.println(Duration.between(start, end).toMillis());
        log.println(Duration.between(start, end).toSeconds());
        log.println(Duration.between(start, end).toMinutes());
    }
}
