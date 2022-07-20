package jv.spark.dataset;

import jv.records.Data;
import jv.records.Document;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serial;
import java.util.*;

public class EvalDocumentUDF implements UDF1<String, List<Data>> {

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
    public List<Data> call(String line) throws Exception {
        int pos = 1, end;
        end = StringUtils.indexOf(line, "\";\"", pos);
        int id = Integer.parseInt(StringUtils.substring(line, pos, end));
        pos = end + 3;
        end = StringUtils.indexOf(line, "\";\"", pos);
        String text = StringUtils.substring(line, pos, end);
        pos = end + 3;
        text = text + " " + StringUtils.substring(line, pos, line.length());
        text = normalize(StringUtils.lowerCase(StringUtils.chop(text)));
        Map<String, Long> counts = new HashMap<>();
        int total = 0;
        for (String term : StringUtils.split(text, ' ')) {
            if (!stopwords.contains(term) && StringUtils.isNotBlank(term)) {
                counts.put(term, counts.getOrDefault(term, 0L) + 1);
                total += 1;
            }
        }
        Document doc =  new Document(id, counts, total);
        return doc.counts().entrySet().stream().map(e -> {
            double idf = Math.log(n_docs / (double) count.get(e.getKey()));
            double tf = e.getValue() / (double) doc.n_terms();
            return new Data(e.getKey(), doc.id(), tf * idf);
        }).toList();
    }
}
