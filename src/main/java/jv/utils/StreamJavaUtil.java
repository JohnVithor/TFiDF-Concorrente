package jv.utils;

import jv.records.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StreamJavaUtil implements UtilInterface {
    static private final Pattern space_split = Pattern.compile("\\s+");
    static private final Pattern csv_split = Pattern.compile("\";\"");
    static private final Pattern normalize = Pattern.compile("[^\\p{L}\\d ]");
    public Document createDocument(String line, Set<String> stopwords) {
        String[] splits = csv_split.split(line);
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        Map<String, Long> counts =
                Arrays.stream(space_split.split(line))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }

    public String normalize(String text) {
        return normalize.matcher(text).replaceAll("").trim();
    }

    public Set<String> setOfTerms(String line, Set<String> stopwords) {
        String [] splits = csv_split.split(line);
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        return Arrays.stream(space_split.split(text))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
