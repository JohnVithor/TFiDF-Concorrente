package jv.spark.rdd;

import jv.records.Document;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Serial;
import java.util.*;

public class CreateDocumentFunctor implements Function<String, Document> {

    @Serial
    private static final long serialVersionUID = -423947324L;

    private final Set<String> stopwords;

    public CreateDocumentFunctor(Set<String> stopwords) {
        this.stopwords = stopwords;
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
    public Document call(String line) throws Exception {
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
        return new Document(id, counts, total);
    }
}
