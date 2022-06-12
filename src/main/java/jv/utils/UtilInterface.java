package jv.utils;

import jv.records.Document;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface UtilInterface {
    default Set<String> load_stop_words(String stop_words_path) {
        Set<String> result = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            result = Arrays.stream(
                    StringUtils.split(reader.readLine(), ',')
            ).collect(Collectors.toUnmodifiableSet()
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    Document createDocument(String line, Set<String> stopwords);

    String normalize(String source);

    Set<String> setOfTerms(String line, Set<String> stopwords);

    default long compute_mft(Map<String, Long> count,
                             long most_frequent_term_count,
                             List<String> most_frequent_terms
    ) {
        for (Map.Entry<String, Long> entry : count.entrySet()) {
            if (entry.getValue() > most_frequent_term_count) {
                most_frequent_term_count = entry.getValue();
                most_frequent_terms.clear();
                most_frequent_terms.add(entry.getKey());
            } else if (entry.getValue().equals(most_frequent_term_count)) {
                most_frequent_terms.add(entry.getKey());
            }
        }
        return most_frequent_term_count;
    }
}
