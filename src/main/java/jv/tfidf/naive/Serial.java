package jv.tfidf.naive;

import jv.records.Data;
import jv.utils.MyWriter;
import jv.records.Document;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Serial implements TFiDFInterface {
    static private final String tfidf_schema_path = "src/main/resources/tfidf_schema.avsc";
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final org.apache.hadoop.fs.Path output_path;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_1_000_id.csv");
        org.apache.hadoop.fs.Path output_path =
                new org.apache.hadoop.fs.Path("naive_concurrent/devel_1_000_id_tfidf_results.parquet");
        TFiDFInterface tfidf = new Serial(stopwords, util, corpus_path, output_path);
        tfidf.compute();
    }
    public Serial(Set<String> stopworlds, UtilInterface util,
                  Path corpus_path, org.apache.hadoop.fs.Path output_path) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.output_path = output_path;
    }

    @Override
    public void compute_df() {
        try(BufferedReader reader = Files.newBufferedReader(this.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                for (String term: util.setOfTerms(line, this.stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L)+1L);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void compute_tfidf() throws IOException {
        MyWriter myWriter = new MyWriter(HadoopOutputFile.
                fromPath(output_path, new Configuration()),
                new Schema.Parser().parse(new FileInputStream(tfidf_schema_path)));
        try(BufferedReader reader = Files.newBufferedReader(this.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Document doc = util.createDocument(line, stopwords);
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(this.n_docs / (double) this.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    myWriter.write(data);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        myWriter.close();
    }
}
