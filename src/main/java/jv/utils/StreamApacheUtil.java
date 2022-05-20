package jv.utils;

import jv.records.Document;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamApacheUtil implements UtilInterface {
    public Document createDocument(String line, Set<String> stopwords) {
        int pos = 0, end;
        end = StringUtils.indexOf(line, "\";\"", pos);
        int id = Integer.parseInt(StringUtils.substring(line, pos, end).replaceFirst("\"", ""));
        pos = end + 3;
        end = StringUtils.indexOf(line, "\";\"", pos);
        String text = StringUtils.substring(line, pos, end);
        pos = end + 3;
        text = text + " " + StringUtils.substring(line, pos, line.length());
        text = normalize(StringUtils.lowerCase(StringUtils.chop(text)));
        Map<String, Long> counts =
                Arrays.stream(StringUtils.split(text, ' '))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }

    public String normalize(String source) {
        return source.codePoints()
                .filter(c -> Character.isLetterOrDigit(c)
                        || Character.isSpaceChar(c))
                .collect(StringBuilder::new,
                        StringBuilder::appendCodePoint,
                        StringBuilder::append)
                .toString();
    }

    public Set<String> setOfTerms(String line, Set<String> stopwords) {
        int pos = 0, end;
        end = StringUtils.indexOf(line, "\";\"", pos);
        pos = end + 3;
        end = StringUtils.indexOf(line, "\";\"", pos);
        String text = StringUtils.substring(line, pos, end);
        pos = end + 3;
        text = text + " " + StringUtils.substring(line, pos, line.length());
        text = normalize(StringUtils.lowerCase(StringUtils.chop(text)));
        return Arrays.stream(StringUtils.split(text, ' '))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
