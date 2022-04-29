package jv.utils;

import jv.records.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamJavaUtil implements UtilInterface {
    public Document createDocument(String line, Set<String> stopwords) {
        String[] splits = line.split("\";\"");
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        Map<String, Long> counts =
                Arrays.stream(text.split("\\s+"))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }

    public String normalize(String text) {
        return text.replaceAll("[^\\p{L}\\d ]", "").trim();
    }

    public Set<String> setOfTerms(String line, Set<String> stopwords) {
        String [] splits = line.split("\";\"");
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        return Arrays.stream(text.split("\\s+"))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
