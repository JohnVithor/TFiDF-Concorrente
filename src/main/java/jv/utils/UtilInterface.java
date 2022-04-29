package jv.utils;

import jv.records.Document;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public interface UtilInterface {
    default Set<String> load_stop_words(String stop_words_path) {
        Set<String> result = null;
        try(BufferedReader reader = new BufferedReader(new FileReader(stop_words_path))) {
            result = Arrays.stream(
                    StringUtils.split(reader.readLine(),',')
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
}
