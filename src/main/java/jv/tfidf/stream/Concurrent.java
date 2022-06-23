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
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent implements TFiDFInterface {
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private Map<String, Long> count = new HashMap<>();
    private final LongAdder n_docs = new LongAdder();
    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private List<Data> highest_tfidf = new ArrayList<>();
    private List<Data> lowest_tfidf = new ArrayList<>();

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path, int n_threads) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(n_threads));
    }

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/train.csv");
        TFiDFInterface tfidf = new Concurrent(stopwords, util, corpus_path, 12);
        tfidf.compute();
        System.out.println(tfidf.results());
    }

    @Override
    public void compute_df() {
        try (Stream<String> lines = Files.lines(corpus_path)) {
            count = lines
                    .parallel()
                    .peek(s -> n_docs.increment())
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
        final double dn_docs = n_docs.doubleValue();
        try (Stream<String> lines = Files.lines(corpus_path)) {
            MinMaxTermsTFiDF r = lines
                    .parallel()
                    .map(line -> util.createDocument(line, stopwords))
                    .flatMap(doc -> doc.counts().entrySet().stream().map(e -> {
                        double idf = Math.log(dn_docs / (double) count.get(e.getKey()));
                        double tf = e.getValue() / (double) doc.n_terms();
                        return new Data(e.getKey(), doc.id(), tf * idf);
                    }))
                    .collect(new MinMaxTermsTFiDFCollector());
            this.highest_tfidf = r.getHighest_tfidfs().parallelStream().sorted(Comparator.comparingDouble(Data::doc_id)).toList();
            this.lowest_tfidf = r.getLowest_tfidfs().parallelStream().sorted(Comparator.comparingDouble(Data::doc_id)).toList();
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
                this.n_docs.longValue(),
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
