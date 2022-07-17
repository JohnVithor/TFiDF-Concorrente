package jv.spark.rdd;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SetOfTermsFunctor implements FlatMapFunction<String, String> {

    @Serial
    private static final long serialVersionUID = -423947324L;

    private final Set<String> stopwords;

    public SetOfTermsFunctor(Set<String> stopwords) {
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
    public Iterator<String> call(String line) throws Exception {
        int pos = 0, end;
        end = StringUtils.indexOf(line, "\";\"", pos);
        pos = end + 3;
        end = StringUtils.indexOf(line, "\";\"", pos);
        String text = StringUtils.substring(line, pos, end);
        pos = end + 3;
        text = text + " " + StringUtils.substring(line, pos, line.length());
        text = normalize(StringUtils.chop(text));
        Set<String> result = new HashSet<>();
        for (String term : StringUtils.split(text, ' ')) {
            if (!stopwords.contains(term) && StringUtils.isNotBlank(term)) {
                result.add(term);
            }
        }
        return result.iterator();
    }
}
