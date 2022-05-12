package jv.tfidf.optimized;

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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Concurrent implements TFiDFInterface {

    private static class MaxTermInfo {
        private final Set<String> terms;
        private Long count;
        public MaxTermInfo(){
            this.terms = new HashSet<>();
            this.count = 0L;
        }

        public void addTerm(String term) {
            this.terms.add(term);
        }
        public void resetTerms(String term) {
            this.terms.clear();
            this.terms.add(term);
        }

        public void joinTerms(MaxTermInfo info) {
            this.terms.addAll(info.terms);
        }

        public List<String> getTermList() {
            return this.terms.stream().sorted(String::compareToIgnoreCase).toList();
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }

    static private final String endLine = "__END__";
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final int n_threads;
    private final int buffer_size;
    private Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    // statistics info
    private List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private final List<Long> biggest_documents = new ArrayList<>();
    private Long biggest_document_count = 0L;
    private final List<Long> smallest_documents = new ArrayList<>();
    private Long smallest_document_count = Long.MAX_VALUE;
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_1_000_id.csv");
        TFiDFInterface tfidf = new Concurrent(
                stopwords, util, corpus_path, 4, 1000);
        tfidf.compute();
        JSON json = new JSON();
        System.out.println(json.toJSON(tfidf.results()));
    }

    public Concurrent(Set<String> stopworlds, UtilInterface util,
                      Path corpus_path, int n_threads, int buffer_size) {
        this.stopwords = stopworlds;
        this.util = util;
        this.corpus_path = corpus_path;
        this.n_threads = n_threads;
        this.buffer_size = buffer_size;
    }
    @Override
    public void compute_df() throws IOException {
        try(Stream<String> lines = Files.lines(corpus_path)) {
            n_docs = lines.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try(Stream<String> lines = Files.lines(corpus_path)) {
            count = lines
                    .parallel()
                    .map(line -> util.setOfTerms(line, stopwords))
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(token -> token,
                             Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Collector<Map.Entry<String, Long>, MaxTermInfo, MaxTermInfo> col = Collector.of(
                MaxTermInfo::new,
                (info, stringLongEntry) -> {
                    if ( stringLongEntry.getValue() > info.getCount() ) {
                        info.setCount(stringLongEntry.getValue());
                        info.resetTerms(stringLongEntry.getKey());
                    } if ( stringLongEntry.getValue().equals(info.getCount()) ) {
                        info.addTerm(stringLongEntry.getKey());
                    }
                },
                (info1, info2) -> {
                    if (info1.getCount() > info2.getCount()) {
                        return info1;
                    } else if (info1.getCount().equals(info2.getCount())) {
                        info1.joinTerms(info2);
                        return info1;
                    } else {
                        return info2;
                    }
                }
                );

        MaxTermInfo r = count.entrySet().parallelStream().collect(col);

        most_frequent_term_count = r.getCount();
        most_frequent_terms = r.getTermList();
    }

    @Override
    public void compute_tfidf() throws IOException {
        try (Stream<String> lines = Files.lines(corpus_path)) {
            lines
                    .parallel()
                    .map(line -> util.createDocument(line, stopwords))
                    .forEach(doc -> {
                        for (String key : doc.counts().keySet()) {
                            double idf = Math.log(n_docs / (double) count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf * idf);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TFiDFInfo results() {
        this.biggest_documents.sort(Long::compareTo);
        this.smallest_documents.sort(Long::compareTo);
        this.highest_tfidf.sort(Comparator.comparingDouble(Data::value));
        this.lowest_tfidf.sort(Comparator.comparingDouble(Data::value));
        return new TFiDFInfo(
                this.count.size(),
                this.most_frequent_terms,
                this.most_frequent_term_count,
                this.n_docs,
                this.biggest_documents,
                this.biggest_document_count,
                this.smallest_documents,
                this.smallest_document_count,
                this.highest_tfidf,
                this.lowest_tfidf);
    }
}
