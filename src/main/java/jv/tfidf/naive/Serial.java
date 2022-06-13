package jv.tfidf.naive;

import jv.records.Data;
import jv.records.Document;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Serial implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;
    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();
    private Long most_frequent_term_count = 0L;

    public Serial(Set<String> stopworlds, UtilInterface util, Path corpus_path) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
    }

    public static void main(String[] args) {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/train.csv");
        TFiDFInterface tfidf = new Serial(stopwords, util, corpus_path);
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        try (BufferedReader reader = Files.newBufferedReader(this.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                for (String term : util.setOfTerms(line, this.stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L) + 1L);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        most_frequent_term_count = util.compute_mft(
                count, most_frequent_term_count, most_frequent_terms
        );
    }

    @Override
    public void compute_tfidf() {
        double htfidf = 0.0;
        double ltfidf = Double.MAX_VALUE;
        try (BufferedReader reader = Files.newBufferedReader(this.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Document doc = util.createDocument(line, stopwords);
                for (String key : doc.counts().keySet()) {
                    double idf = Math.log(this.n_docs / (double) this.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf * idf);
                    if (data.value() > htfidf) {
                        htfidf = data.value();
                        highest_tfidf.clear();
                        highest_tfidf.add(data);
                    } else if (data.value() == htfidf) {
                        highest_tfidf.add(data);
                    }
                    if (data.value() < ltfidf) {
                        ltfidf = data.value();
                        lowest_tfidf.clear();
                        lowest_tfidf.add(data);
                    } else if (data.value() == ltfidf) {
                        lowest_tfidf.add(data);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TFiDFInfo results() {
        this.most_frequent_terms.sort(String::compareTo);
        this.highest_tfidf.sort(Comparator.comparingDouble(Data::value));
        this.lowest_tfidf.sort(Comparator.comparingDouble(Data::value));
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
