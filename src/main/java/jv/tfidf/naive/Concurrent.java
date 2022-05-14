package jv.tfidf.naive;

import jv.records.TFiDFInfo;
import jv.utils.MyBuffer;
import jv.records.Data;
import jv.records.Document;
import jv.tfidf.TFiDFInterface;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Concurrent implements TFiDFInterface {
    static private final String endLine = "__END__";
    private final Set<String> stopwords;
    private final UtilInterface util;
    private final Path corpus_path;
    private final int n_threads;
    private final int buffer_size;
    private final Map<String, Long> count = new HashMap<>();
    private long n_docs = 0L;

    // statistics info
    private final List<String> most_frequent_terms = new ArrayList<>();
    private Long most_frequent_term_count = 0L;
    private final List<Data> highest_tfidf = new ArrayList<>();
    private final List<Data> lowest_tfidf = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        UtilInterface util = new ForEachApacheUtil();
        Set<String> stopwords = util.load_stop_words("datasets/stopwords.txt");
        java.nio.file.Path corpus_path = Path.of("datasets/devel_100_000_id.csv");
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
        List<Thread> threads = new ArrayList<>();
        List<Map<String, Long>> counts = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        for (int i = 0; i < n_threads; ++i) {
            Map<String, Long> count_i = new HashMap<>();
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        for (String term: util.setOfTerms(line, stopwords)) {
                            count_i.put(term, count_i.getOrDefault(term, 0L)+1L);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
            counts.add(count_i);
        }
        try(BufferedReader reader = Files.newBufferedReader(corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                ++n_docs;
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (Thread t : threads) {
                buffer.put(endLine);
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        for (Map<String, Long> c : counts) {
            for (Map.Entry<String, Long> pair : c.entrySet()) {
                count.put(pair.getKey(), count.getOrDefault(pair.getKey(), 0L) + pair.getValue());
            }
        }
        for (Map.Entry<String, Long> entry: this.count.entrySet()) {
            if (entry.getValue() > most_frequent_term_count) {
                most_frequent_term_count = entry.getValue();
                most_frequent_terms.clear();
                most_frequent_terms.add(entry.getKey());
            } else if (entry.getValue().equals(most_frequent_term_count)) {
                most_frequent_terms.add(entry.getKey());
            }
        }
    }

    @Override
    public void compute_tfidf() throws IOException {
        final ArrayList<ArrayList<Data>> data_high = new ArrayList<>();
        final ArrayList<ArrayList<Data>> data_low = new ArrayList<>();
        final double[] htfidf = new double[n_threads];
        final double[] ltfidf = new double[n_threads];
        for (int i = 0; i < n_threads; i++) {
            data_high.add(new ArrayList<>());
            data_low.add(new ArrayList<>());
            ltfidf[i] = Double.MAX_VALUE;
        }
        List<Thread> threads = new ArrayList<>();
        MyBuffer<String> buffer = new MyBuffer<>(buffer_size);
        long finalN_docs = n_docs;
        for (int i = 0; i < n_threads; ++i) {
            int finalI = i;
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        String line = buffer.take();
                        if (line == endLine) {
                            return;
                        }
                        Document doc = util.createDocument(line, stopwords);
                        for (String key: doc.counts().keySet()) {
                            double idf = Math.log(finalN_docs / (double) count.get(key));
                            double tf = doc.counts().get(key) / (double) doc.n_terms();
                            Data data = new Data(key, doc.id(), tf*idf);

                            if (data.value() > htfidf[finalI]) {
                                htfidf[finalI] = data.value();
                                data_high.get(finalI).clear();
                                data_high.get(finalI).add(data);
                            } else if (data.value() == htfidf[finalI]) {
                                data_high.get(finalI).add(data);
                            }
                            if (data.value() < ltfidf[finalI]) {
                                ltfidf[finalI] = data.value();
                                data_low.get(finalI).clear();
                                data_low.get(finalI).add(data);
                            } else if (data.value() == ltfidf[finalI]) {
                                data_low.get(finalI).add(data);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        try(BufferedReader reader = Files.newBufferedReader(corpus_path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.put(line);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            for (Thread t : threads) {
                buffer.put(endLine);
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        double htfidf_final = 0.0;
        double ltfidf_final = Double.MAX_VALUE;
        for (int i = 0; i < n_threads; ++i) {
            if (htfidf[i] > htfidf_final) {
                htfidf_final = htfidf[i];
                highest_tfidf.clear();
                highest_tfidf.addAll(data_high.get(i));
            } else if (htfidf[i] == htfidf_final) {
                highest_tfidf.addAll(data_high.get(i));
            }
            if (ltfidf[i] < ltfidf_final) {
                ltfidf_final = ltfidf[i];
                lowest_tfidf.clear();
                lowest_tfidf.addAll(data_low.get(i));
            } else if (ltfidf[i] == ltfidf_final) {
                lowest_tfidf.addAll(data_low.get(i));
            }
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
