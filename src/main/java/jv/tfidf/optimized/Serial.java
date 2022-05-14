package jv.tfidf.optimized;

import jv.collectors.MaxTermCount;
import jv.collectors.MaxTermCountCollector;
import jv.collectors.MinMaxTermsTFiDF;
import jv.collectors.MinMaxTermsTFiDFCollector;
import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Serial implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;
    private final Object lock = new Object();
    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private List<Data> highest_tfidf = new ArrayList<>();
    private List<Data> lowest_tfidf = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_100_000_id.csv");
        TFiDFInterface tfidf = new jv.tfidf.optimized.Serial(stopwords, util, corpus_path);
        tfidf.compute();
        JSON json = new JSON();
        System.out.println(json.toJSON(tfidf.results()));
    }

    public Serial(Set<String> stopworlds, UtilInterface util, Path corpus_path) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
    }

    @Override
    public void compute_df() {
        try (Stream<String> lines = Files.lines(corpus_path)) {
            count = lines
                    .sequential()
                    .peek(s -> {
                        synchronized (lock) {
                            ++n_docs;
                        }
                    })
                    .flatMap(line -> util.setOfTerms(line, stopwords).stream())
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MaxTermCount r = this.count.entrySet()
                .stream().sequential().collect(new MaxTermCountCollector());
        most_frequent_term_count = r.getMax_count();
        most_frequent_terms = r.getTerms().stream().sequential().sorted().toList();
    }

    @Override
    public void compute_tfidf() throws IOException {
        try(Stream<String> lines = Files.lines(corpus_path)) {
            MinMaxTermsTFiDF r = lines
                .sequential()
                .map(line -> util.createDocument(line, stopwords))
                .flatMap(doc -> doc.counts().entrySet().stream().map(e -> {
                    double idf = Math.log(n_docs / (double) count.get(e.getKey()));
                    double tf = e.getValue() / (double) doc.n_terms();
                    return new Data(e.getKey(), doc.id(), tf*idf);
                }))
                .collect(new MinMaxTermsTFiDFCollector());
            this.highest_tfidf = r.getHighest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
            this.lowest_tfidf = r.getLowest_tfidfs().stream().sorted(Comparator.comparingDouble(Data::value)).toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TFiDFInfo results() {
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
