package main;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";
    private static String filename = "test_id";
    static private String input_path = "datasets/"+filename+".csv";
    static private String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private String tfidf_out_fileName = "results_concurrent/" + filename+ "_tfidf_results.parquet";
    static private String log_output = "logs_concurrent/output_"+filename+".log";

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {

            }
        }));
        filename = args[0];
        filename = "test_id";
        input_path = "datasets/" + filename + ".csv";
        tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        tfidf_out_fileName = "results_concurrent/" + filename + "_tfidf_results.parquet";
        log_output = "logs_concurrent/output_" + filename + ".log";
        Instant start = Instant.now();
        Set<String> stopwords = Utils.load_stop_words(stop_words_path);
        AtomicInteger n_docs = new AtomicInteger();
        Map<String, Long> count;
        try(Stream<String> lines = Files.lines(Path.of(input_path))) {
            count = lines
                    .parallel()
                    .map(line -> {
                        n_docs.getAndIncrement();
                        int pos = 0, end;
                        end = StringUtils.indexOf(line,"\",\"", pos);
                        pos = end + 1;
                        end = StringUtils.indexOf(line,"\",\"", pos);
                        pos = end + 1;
                        end = StringUtils.indexOf(line,"\",\"", pos);
                        String text = StringUtils.substring(line, pos, end);
                        text = Utils.normalize(StringUtils.lowerCase(StringUtils.chop(text)));
                        return Arrays.stream(StringUtils.split(text,' '))
                                .sequential()
                                .filter(e -> !stopwords.contains(e))
                                .collect(Collectors.toUnmodifiableSet());
                    })
                    .flatMap(Set::stream).collect(Collectors
                            .groupingBy(token -> token, Collectors.counting()));;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Instant mid = Instant.now();
        System.out.println(Duration.between(start, mid).toMillis());
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));

        try(Stream<String> lines = Files.lines(Path.of(input_path))) {
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
        System.out.println(Duration.between(mid, end).toMillis());
        System.out.println(Duration.between(start, end).toMillis());
    }
}
