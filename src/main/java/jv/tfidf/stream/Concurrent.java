package jv.tfidf.stream;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.stream.collectors.MaxTermCount;
import jv.tfidf.stream.collectors.MaxTermCountCollector;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDF;
import jv.tfidf.stream.collectors.MinMaxTermsTFiDFCollector;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final Object lock = new Object();
    private Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;
    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private List<Data> highest_tfidf = new ArrayList<>();
    private List<Data> lowest_tfidf = new ArrayList<>();

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_100_000_id.csv");
        TFiDFInterface tfidf = new Concurrent(stopwords, util, corpus_path);
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        try (Stream<String> lines = Files.lines(corpus_path)) {
            count = lines
                    .parallel()
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
                .stream().parallel().collect(new MaxTermCountCollector());
        most_frequent_term_count = r.getMax_count();
        most_frequent_terms = r.getTerms().stream().sequential().sorted().toList();
    }

    @Override
    public void compute_tfidf() {
        try (Stream<String> lines = Files.lines(corpus_path)) {
            MinMaxTermsTFiDF r = lines
                    .parallel()
                    .map(line -> util.createDocument(line, stopwords))
                    .flatMap(doc -> doc.counts().entrySet().stream().map(e -> {
                        double idf = Math.log(n_docs / (double) count.get(e.getKey()));
                        double tf = e.getValue() / (double) doc.n_terms();
                        return new Data(e.getKey(), doc.id(), tf * idf);
                    }))
                    .collect(new MinMaxTermsTFiDFCollector());
            this.highest_tfidf = r.getHighest_tfidfs().parallelStream().sorted(Comparator.comparingDouble(Data::value)).toList();
            this.lowest_tfidf = r.getLowest_tfidfs().parallelStream().sorted(Comparator.comparingDouble(Data::value)).toList();
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
