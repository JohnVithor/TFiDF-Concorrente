package simplified;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String filename = "test_id";
//    static private final String filename = "devel_100_000_id";
    static private final String input_path = "datasets/"+filename+".csv";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String tfidf_out_fileName = "results_concurrent/" + filename+ "_tfidf_results.parquet";
    static private final String log_output = "logs_concurrent/output_"+filename+".log";

    record Document(int id, Map<String, Long> counts, long n_terms) {}

    public static void main(String[] args) throws IOException, InterruptedException {
        Instant start = Instant.now();
        Set<String> stopwords = load_stop_words();
        List<Document> docs;
        try(Stream<String> lines = Files.lines(Path.of(input_path))) {
            docs = lines
                    .parallel()
                    .map(line -> {
                String[] cells = line.split("\",\"");
                int id = Integer.parseInt(cells[0].replaceFirst("\"", ""));
                String title = cells[1];
                String text = cells[2].substring(0, cells[2].length() - 1).toLowerCase();
                Map<String, Long> counts =
                        Arrays.stream(text.replaceAll("[^a-zA-Z\\d ]", "")
                                        .split("\\s+")).sequential().filter(e -> !stopwords.contains(e))
                                .collect(Collectors.groupingBy(e -> e, Collectors.counting()));
                return new Document(id, counts, counts.values().stream().mapToLong(value -> value).sum());
            }).toList();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Instant p1 = Instant.now();
        System.out.println(Duration.between(start, p1).toMillis());

        Map<String, Long> count = new HashMap<>();
        for (Document doc : docs) {
            for (String term: doc.counts().keySet()) {
                count.put(term, count.getOrDefault(term, 0L) + 1);
            }
        }
        Instant p2 = Instant.now();
        System.out.println(Duration.between(p1, p2).toMillis());

        int n_docs = docs.size();

        Data end = new Data(null,0,0.0);

        ConcurrentLinkedQueue<Data> buffer = new ConcurrentLinkedQueue<>();
//        Buffer<Data> buffer = new Buffer<>(10);

        RecordConsumer recordConsumer = new RecordConsumer(
                tfidf_out_fileName,
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)),
                end,
                buffer
        );
        Thread consumer = new Thread(recordConsumer);
        consumer.start();

        docs.stream().parallel().forEach(doc -> {
            for (String key: doc.counts().keySet()) {
                double idf = Math.log( n_docs / (double) count.get(key));
                double tf = doc.counts().get(key) / (double) doc.n_terms;
                Data data = new Data(key, doc.id(), idf*tf);
                buffer.add(data);
            }
        });
        buffer.add(end);
        consumer.join();
        Instant p3 = Instant.now();
        System.out.println(Duration.between(p2, p3).toMillis());
    }

    private static Set<String> load_stop_words() {
        Set<String> result = null;
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            result = Arrays.stream(reader.readLine().split(",")).collect(Collectors.toUnmodifiableSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
