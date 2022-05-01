package jv.microbenchmark;

import jv.utils.*;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@State(Scope.Benchmark)
public class ExecutionPlan {
//    @Param({"train_id"})
//    @Param({"devel_1_000_id", "devel_10_000_id", "devel_100_000_id", "test_id", "train_id"})
    @Param({"devel_100_000_id", "test_id", "train_id"})
    public String dataset;
//    @Param({"foreach_java", "foreach_apache", "stream_java", "stream_apache"})
    @Param({"foreach_java", "foreach_apache"})
//    @Param({"foreach_apache"})
    public String stringManipulation;
    public UtilInterface util;
    public String stop_words_path = "datasets/stopwords.txt";
    public Path input_path;
    public Set<String> stopwords;
    public Map<String, Long> count = new HashMap<>();
    public AtomicInteger n_docs = new AtomicInteger(0);
    public Path text_input = Path.of("datasets/devel_1_000_id.csv");

    public final Pattern space_split = Pattern.compile("\\s+");
    public final Pattern csv_split = Pattern.compile("\";\"");
    public final Pattern normalize = Pattern.compile("[^\\p{L}\\d ]");

    @Setup(Level.Iteration)
    public void setUp() {
        input_path = Path.of("datasets/" + dataset + ".csv");
        switch (stringManipulation) {
            case "foreach_java" -> util = new ForEachJavaUtil();
            case "foreach_apache" -> util = new ForEachApacheUtil();
            case "stream_java" -> util = new StreamJavaUtil();
            default -> util = new StreamApacheUtil();
        }

        // preparação para a segunda etapa do algoritmo
        UtilInterface util = new ForEachApacheUtil();
        stopwords = util.load_stop_words(stop_words_path);
        try(Stream<String> lines = Files.lines(input_path)) {
            count = lines
                    .parallel()
                    .peek(e -> n_docs.getAndIncrement())
                    .map(line -> util.setOfTerms(line, stopwords))
                    .flatMap(Set::stream)
                    .collect(Collectors.groupingBy(token -> token,
                            Collectors.counting())
                    );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
