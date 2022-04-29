package jv.utils;

import jv.records.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ForEachJavaUtil implements UtilInterface {
    public Document createDocument(String line, Set<String> stopwords) {
        String[] splits = line.split("\";\"");
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        String[] terms = text.split("\\s+");
        Map<String, Long> counts = new HashMap<>();
        int total = 0;
        for (String term: terms) {
            if(!stopwords.contains(term)) {
                counts.put(term, counts.getOrDefault(term,0L) + 1);
                total+=1;
            }
        }
        return new Document(id, counts, total);
    }
    public String normalize(String text) {
        return text.replaceAll("[^\\p{L}\\d ]", "").trim();
    }
    public Set<String> setOfTerms(String line, Set<String> stopwords) {
        String [] splits = line.split("\";\"");
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = normalize(text.toLowerCase());
        String[] terms = text.split("\\s+");
        Set<String> result = new HashSet<>();
        for (String term: terms) {
            if(!stopwords.contains(term)) {
                result.add(term);
            }
        }
        return result;
    }
}
