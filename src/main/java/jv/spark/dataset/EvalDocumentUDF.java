package jv.spark.dataset;

import jv.records.Data;
import jv.records.Document;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serial;
import java.util.*;

public class EvalDocumentUDF implements UDF1<String, Map<String,Double>> {

    @Serial
    private static final long serialVersionUID = -465848;

    private final Set<String> stopwords;
    private final Map<String, Long> count;
    private final long n_docs;

    public EvalDocumentUDF(Set<String> stopwords,
                           Map<String, Long> count,
                           long n_docs) {
        this.stopwords = stopwords;
        this.count = count;
        this.n_docs = n_docs;
    }

    public String normalize(String source) {
        StringBuilder result = new StringBuilder();
        char one;
        for (int i = 0; i < source.length(); ++i) {
            one = source.charAt(i);
            if (Character.isLetterOrDigit(one) || Character.isSpaceChar(one)) {
                result.append(Character.toLowerCase(one));
            }
        }
        return result.toString();
    }

    @Override
    public Map<String,Double> call(String line) throws Exception {
        String text = normalize(line);
        Map<String, Long> counts = new HashMap<>();
        int total = 0;
        for (String term : StringUtils.split(text, ' ')) {
            if (!stopwords.contains(term) && StringUtils.isNotBlank(term)) {
                counts.put(term, counts.getOrDefault(term, 0L) + 1);
                total += 1;
            }
        }
        int finalTotal = total;

        Map<String, Double> tfidfs = new HashMap<>();
        counts.forEach((s, aLong) -> {
            double idf = Math.log(n_docs / (double) count.get(s));
            double tf = aLong / (double) finalTotal;
            tfidfs.put(s, tf*idf);
        });

        return tfidfs;
    }
}
