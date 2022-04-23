package main.nonoptimized;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    public record Document(int id, Map<String, Long> counts, long n_terms) {}

    public static Set<String> load_stop_words(String stop_words_path) {
        Set<String> result = null;
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            result = Arrays.stream(
                    reader.readLine().split("\\s+")
                    ).collect(Collectors.toUnmodifiableSet()
                    );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Document createDocument(String line, Set<String> stopwords) {
        String[] splits = line.split("\",\"");
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[2].substring(0, splits[2].length() - 1).toLowerCase();
        text = Utils.normalize(text);
        Map<String, Long> counts =
                Arrays.stream(text.split("\\s+"))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }

    public static String normalize(String text) {
        return text.replaceAll("[^a-zA-Z\\d ]", "");
    }

    public static Set<String> setOfTerms(String line, Set<String> stopwords) {
        String text = line.split("\",\"")[2];
        text = text.substring(0, text.length() - 1).toLowerCase();
        text = Utils.normalize(text);
        return Arrays.stream(text.split("\\s+"))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
