package jv.tfidf.naive;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.records.Document;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Serial implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    // statistics info
    private String most_frequent_term ="";
    private String less_frequent_term ="";
    private long biggest_document = 0;
    private long smallest_document = 0;
    private Data highest_tfidf = new Data("",-1,0);
    private Data lowest_tfidf = new Data("",-1,Integer.MAX_VALUE);

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_1_000_id.csv");
        TFiDFInterface tfidf = new Serial(stopwords, util, corpus_path);
        tfidf.compute();
    }
    public Serial(Set<String> stopworlds, UtilInterface util, Path corpus_path) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
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
        long mft = 0;
        long lft = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry: this.count.entrySet()) {
            if (entry.getValue() > mft) {
                mft = entry.getValue();
                most_frequent_term = entry.getKey();
            } else if (entry.getValue() < lft) {
                lft = entry.getValue();
                less_frequent_term = entry.getKey();
            }
        }
    }

    @Override
    public void compute_tfidf() throws IOException {
        long bgc=0;
        long sbc=Long.MAX_VALUE;
        try(BufferedReader reader = Files.newBufferedReader(this.corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Document doc = util.createDocument(line, stopwords);
                if (doc.n_terms() > bgc) {
                    bgc = doc.n_terms();
                    biggest_document = doc.id();
                } else if (doc.n_terms() < sbc) {
                    sbc = doc.n_terms();
                    smallest_document = doc.id();
                }
                for (String key: doc.counts().keySet()) {
                    double idf = Math.log(this.n_docs / (double) this.count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf*idf);
                    if (data.value() > highest_tfidf.value()) {
                        highest_tfidf = data;
                    } else if (data.value() > lowest_tfidf.value()) {
                        lowest_tfidf = data;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TFiDFInfo results() {
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_term,
                this.less_frequent_term,
                this.biggest_document,
                this.smallest_document,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
