package jv.tfidf.executor.callable;

import jv.records.Data;
import jv.records.Document;
import jv.utils.MyBuffer;
import jv.utils.UtilInterface;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class TaskTFiDFConsumer implements Callable<Pair<ArrayList<Data>, ArrayList<Data>>> {

    private final MyBuffer<String> buffer;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final String endline;
    private final Map<String, Long> count;

    private final long n_docs;
    private final ArrayList<Data> data_high = new ArrayList<>();
    private final ArrayList<Data> data_low = new ArrayList<>();
    private double htfidf;
    private double ltfidf = Double.MAX_VALUE;

    public TaskTFiDFConsumer(MyBuffer<String> buffer,
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
    public Pair<ArrayList<Data>, ArrayList<Data>> call() {
        try {
            while (true) {
                String line = buffer.take();
                if (line == endline) {
                    return Pair.of(data_low, data_high);
                }
                Document doc = util.createDocument(line, stopwords);
                for (String key : doc.counts().keySet()) {
                    double idf = Math.log(n_docs / (double) count.get(key));
                    double tf = doc.counts().get(key) / (double) doc.n_terms();
                    Data data = new Data(key, doc.id(), tf * idf);

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
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
