package jv.tfidf.collections.threads;

import jv.records.Data;
import jv.records.Document;
import jv.utils.MyBuffer;
import jv.utils.UtilInterface;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerThreadTFiDF extends Thread {

    private final LinkedBlockingQueue<String> buffer;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final String endline;
    private final Map<String, Long> count;

    private final long n_docs;
    private final ArrayList<Data> data_high = new ArrayList<>();
    private final ArrayList<Data> data_low = new ArrayList<>();
    private double htfidf;
    private double ltfidf = Double.MAX_VALUE;

    public ConsumerThreadTFiDF(LinkedBlockingQueue<String> buffer,
                               UtilInterface util,
                               Set<String> stopwords,
                               String endline,
                               Map<String, Long> count,
                               long n_docs) {
        this.buffer = buffer;
        this.util = util;
        this.stopwords = stopwords;
        this.endline = endline;
        this.count = count;
        this.n_docs = n_docs;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String line = buffer.take();
                if (line == endline) {
                    return;
                }
                Document doc = util.createDocument(line, stopwords);
                for (String key : doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf * idf);
                    findMaxMin(data);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void findMaxMin(Data data) {
        if (data.value() > htfidf) {
            htfidf = data.value();
            data_high.clear();
            data_high.add(data);
        } else if (data.value() == htfidf) {
            data_high.add(data);
        }
        if (data.value() < ltfidf) {
            ltfidf = data.value();
            data_low.clear();
            data_low.add(data);
        } else if (data.value() == ltfidf) {
            data_low.add(data);
        }
    }

    public double getHtfidf() {
        return htfidf;
    }

    public double getLtfidf() {
        return ltfidf;
    }

    public ArrayList<Data> getData_high() {
        return data_high;
    }

    public ArrayList<Data> getData_low() {
        return data_low;
    }
}
