package jv.utils;

import jv.records.Document;

import java.util.*;
import java.util.regex.Pattern;

public class ForEachJavaUtil implements UtilInterface {
    static private final Pattern space_split = Pattern.compile("\\s+");
    static private final Pattern csv_split = Pattern.compile("\";\"");
    static private final Pattern normalize = Pattern.compile("[^\\p{L}\\d ]");

    public Document createDocument(String line, Set<String> stopwords) {
        String[] splits = csv_split.split(line);
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        String[] terms = space_split.split(text);
        Map<String, Long> counts = new HashMap<>();
        int total = 0;
        for (String term: terms) {
            if(!stopwords.contains(term) && !term.isBlank()) {
                counts.put(term, counts.getOrDefault(term,0L) + 1);
                total+=1;
            }
        }
        return new Document(id, counts, total);
    }
    public String normalize(String text) {
        return normalize.matcher(text).replaceAll("").trim();
    }
    public Set<String> setOfTerms(String line, Set<String> stopwords) {
        String [] splits = csv_split.split(line);
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        String[] terms = space_split.split(text);
        Set<String> result = new HashSet<>();
        for (String term: terms) {
            if(!stopwords.contains(term) && !term.isBlank()) {
                result.add(term);
            }
        }
        return result;
    }
}
