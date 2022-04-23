package main.optimized;

import org.apache.commons.lang.StringUtils;

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
            result = Arrays.stream(
                    StringUtils.split(reader.readLine(),',')
                    ).collect(Collectors.toUnmodifiableSet()
                    );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Document createDocument(String line, Set<String> stopwords) {
        int pos = 0, end;
        end = StringUtils.indexOf(line,"\",\"", pos);
        int id = Integer.parseInt(StringUtils.substring(line, pos, end).replaceFirst("\"", ""));
        pos = end + 1;
        end = StringUtils.indexOf(line,"\",\"", pos);
        String title = StringUtils.substring(line, pos, end);
        pos = end + 1;
        end = StringUtils.indexOf(line,"\",\"", pos);
        String text = StringUtils.substring(line, pos, end);
        text = Utils.normalize(StringUtils.lowerCase(StringUtils.chop(text)));
        Map<String, Long> counts =
                Arrays.stream(StringUtils.split(text,' '))
                        .sequential()
                        .filter(e -> !stopwords.contains(e))
                        .collect(Collectors.groupingBy(e -> e,
                                Collectors.counting()));
        return new Document(id, counts, counts.values().stream()
                .sequential().mapToLong(value -> value).sum());
    }

    public static String normalize(String source) {
        StringBuilder result = new StringBuilder();
        char one;
        for (int i = 0; i < source.length(); ++i) {
            one = source.charAt(i);
            if (Character.isLetterOrDigit(one) || Character.isSpaceChar(one)) {
                result.append(one);
            }
        }
        return result.toString();
    }
    public static Set<String> setOfTerms(String line, Set<String> stopwords) {
        int pos = 0, end;
        end = StringUtils.indexOf(line,"\",\"", pos);
        pos = end + 1;
        end = StringUtils.indexOf(line,"\",\"", pos);
        pos = end + 1;
        end = StringUtils.indexOf(line,"\",\"", pos);
        String text = StringUtils.substring(line, pos, end);
        text = Utils.normalize(StringUtils.lowerCase(StringUtils.chop(text)));
        return Arrays.stream(StringUtils.split(text,' '))
                .sequential()
                .filter(e -> !stopwords.contains(e))
                .collect(Collectors.toUnmodifiableSet());
    }
}
