package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {

    public record Document(int id, Map<String, Long> counts, long n_terms) {}

    public static Set<String> load_stop_words(String stop_words_path) {
        Set<String> result = null;
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            result = Arrays.stream(reader.readLine().
                    split(",")).collect(Collectors.toUnmodifiableSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Document createDocument(String line, Set<String> stopwords) {
        String[] cells = line.split("\",\"");
        int id = Integer.parseInt(cells[0].replaceFirst("\"", ""));
        String title = cells[1];
        String text = cells[2].substring(0, cells[2].length() - 1).toLowerCase();
        Map<String, Long> counts =
                Arrays.stream(text.replaceAll("[^a-zA-Z\\d ]", "")
                                .split("\\s+"))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }
}
