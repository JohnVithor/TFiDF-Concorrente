package main;

import org.apache.avro.Schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentMain {
    static private final String stop_words_path = "datasets/stopwords.txt";
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    static private final String filename = "test_id";
    static private final String input_path = "datasets/"+filename+".csv";
    static private final String tfidf_out_fileName = "results_concurrent/" + filename+ "_tfidf_results.parquet";
    static private final String log_output = "logs_concurrent/output_"+filename+".log";
    public static void main(String[] args) throws FileNotFoundException {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {/*Descarta o log*/}
        }));
        run();
    }

    public static void run() throws FileNotFoundException {
        PrintStream output = new PrintStream(new OutputStream() {
            final FileOutputStream f = new FileOutputStream(log_output);
            @Override
            public void write(int b) throws IOException {
                f.write(b);
                System.out.write(b);
            }
        });
        Instant start = Instant.now();
        load_stop_words();
        try {
            List<ConcurrentDocument> documentList = getDocumentList();
            output.println("Vocabulary Size: " + ConcurrentDocument.vocab_size);
            output.println("Number of Documents: " + documentList.size());

            double[] terms_count_all_docs = getTerms_count_all_docs(documentList);
            process(documentList, terms_count_all_docs);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Duration d = Duration.between(start, Instant.now());
        output.println("Total Time in nanoseconds: " + d.toNanos());
        output.println("Total Time in milliseconds: " + d.toMillis());
        output.println("Total Time in seconds: " + d.toSeconds());
        output.println("Total Time in minutes: " + d.toMinutes());
    }
    private static void load_stop_words() {
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            ConcurrentDocument.stopwords.addAll(Arrays.stream(reader.readLine().split(",")).toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<ConcurrentDocument> getDocumentList() throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(input_path))) {
            return lines.parallel()
                    .map(line -> {
                        String [] cells = line.split("\",\"");
                        return new ConcurrentDocument(Integer.parseInt(
                                cells[0].replaceFirst("\"", "")), cells[1], cells[2]);
                    }).collect(Collectors.toList());
        }
    }

    private static double[] getTerms_count_all_docs(List<ConcurrentDocument> documentList) {
        List<Object> locks = Collections.nCopies(ConcurrentDocument.vocab_size,new Object());
        double[] terms_count_all_docs = new double[ConcurrentDocument.vocab_size];
        documentList.parallelStream().forEach(concurrentDocument -> {
            for (Integer key : concurrentDocument.getFrequency_table().keySet()) {
                synchronized (locks.get(key)){
                    terms_count_all_docs[key] += 1;
                }
            }
        });
        return terms_count_all_docs;
    }
    public static void process(List<ConcurrentDocument> documentList, double[] terms_count_all_docs) throws IOException, InterruptedException {
        Schema schema_tfidf = new Schema.Parser().parse(new FileInputStream(tfidf_schema_path));
        int doc_len = documentList.size();
        Buffer<Data> buffer = new Buffer<>(1000);
        Data end = new Data(null, 0, 0.0);
        RecordConsumer rc = new RecordConsumer(tfidf_out_fileName, schema_tfidf, end, buffer);
        Thread thread = new Thread(rc);
        thread.start();
        documentList.parallelStream().forEach(concurrentDocument ->
                processDocument(terms_count_all_docs,
                        doc_len, concurrentDocument, buffer));
        buffer.add(end);
        thread.join();
    }
    private static void processDocument(double[] terms_count_all_docs,
                                        int doc_len,
                                        ConcurrentDocument doc,
                                        Buffer<Data> buffer) {
        for (Integer key: doc.getFrequency_table().keySet()) {
            double idf = Math.log(doc_len / terms_count_all_docs[key]);
            double tf = doc.calculateTermFrequency(key);
            Data data = new Data(ConcurrentDocument.id_token_vocabulary.get(key),
                    doc.getId(), tf*idf);
            try {
                buffer.add(data);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
