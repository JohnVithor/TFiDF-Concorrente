package jv.utils;

import jv.records.Document;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ForEachApacheUtil implements UtilInterface {
    public Document createDocument(String line, Set<String> stopwords) {
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

    public Set<String> setOfTerms(String line, Set<String> stopwords) {
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
        return result;
    }
}
