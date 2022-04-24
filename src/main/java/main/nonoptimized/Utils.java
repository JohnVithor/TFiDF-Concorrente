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
                    reader.readLine().split(",")
                    ).collect(Collectors.toUnmodifiableSet()
                    );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Document createDocument(String line, Set<String> stopwords) {
        String[] splits = line.split("\";\"");
        int id = Integer.parseInt(splits[0].replaceFirst("\"", ""));
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = Utils.normalize(text.toLowerCase());
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
//        return text;
        return text.replaceAll("[^\\p{L}\\d ]", "").trim();
    }

    public static Set<String> setOfTerms(String line, Set<String> stopwords) {
        String [] splits = line.split("\";\"");
        String text = splits[1] + " " + splits[2].substring(0, splits[2].length() - 1);
        text = Utils.normalize(text.toLowerCase());
        return Arrays.stream(text.split("\\s+"))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
