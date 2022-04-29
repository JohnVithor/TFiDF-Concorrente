package jv.tfidf.naive;

import jv.records.Data;
import jv.MyWriter;
import jv.utils.ForEachJavaUtil;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Serial {
    static private final String stop_words_path = "datasets/stopwords.txt";
    private final Path input_path;
    private final String tfidf_out_fileName;
    private Set<String> stopwords;
    private Map<String, Long> count;
    private int n_docs;
    public Serial(String target) {
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {}
        }));
        input_path = Path.of("datasets/" + target + ".csv");
        tfidf_out_fileName = "serial_naive/" + target + "_tfidf_results.parquet";
    }

    public void firstHalf() throws IOException {
        count = new HashMap<>();
        n_docs = 0;
        stopwords = ForEachJavaUtil.load_stop_words(stop_words_path);
        try(Stream<String> lines = Files.lines(input_path)) {
            List<String> stringList = lines.toList();
            n_docs = stringList.size();
            for (String line: stringList) {
                for (String term: ForEachJavaUtil.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void secondHalfWriting() throws IOException {
        String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(new org.apache.hadoop.fs.Path(tfidf_out_fileName),
                        new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));
        try(Stream<String> lines = Files.lines(input_path)) {
            for (String line: lines.toList()) {
                ForEachJavaUtil.Document doc = ForEachJavaUtil.createDocument(line, stopwords);
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
    public void secondHalfNotWriting(Blackhole blackhole) {
        try(Stream<String> lines = Files.lines(input_path)) {
            for (String line: lines.toList()) {
                ForEachJavaUtil.Document doc = ForEachJavaUtil.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    blackhole.consume(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}