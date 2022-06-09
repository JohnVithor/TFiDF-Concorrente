package jv.tfidf.executor.callable;

import jv.records.Data;
import jv.records.Document;
import jv.utils.UtilInterface;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class TaskLowerHigherTFiDF implements Callable<Pair<ArrayList<Data>, ArrayList<Data>>> {

    private final String line;
    private final Long n_docs;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final Map<String, Long> count;
    
    public TaskLowerHigherTFiDF(String line,
                                Long n_docs,
                                UtilInterface util,
                                Set<String> stopwords,
                                Map<String, Long> count
    ) {
        this.line = line;
        this.n_docs = n_docs;
        this.util = util;
        this.stopwords = stopwords;
        this.count = count;
    }

    @Override
    public Pair<ArrayList<Data>, ArrayList<Data>> call() throws Exception {
        ArrayList<Data> data_high = new ArrayList<>();
        ArrayList<Data> data_low = new ArrayList<>();
        double htfidf = 0;
        double ltfidf = Double.MAX_VALUE;
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
        return Pair.of(data_low, data_high);
    }
}
