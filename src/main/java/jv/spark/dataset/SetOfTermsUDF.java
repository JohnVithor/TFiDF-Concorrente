package jv.spark.dataset;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

public class SetOfTermsUDF implements UDF1<String, List<String>>{

    @Serial
    private static final long serialVersionUID = -10924321L;

    private Set<String> stopwords;

    public SetOfTermsUDF(Set<String> stopwords) {
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
    public List<String> call(String line) throws Exception {
        String text = normalize(StringUtils.chop(line));
        Set<String> result = new HashSet<>();
        for (String term : StringUtils.split(text, ' ')) {
            if (!stopwords.contains(term) && StringUtils.isNotBlank(term)) {
                result.add(term);
            }
        }
        return result.stream().toList();
    }
}
