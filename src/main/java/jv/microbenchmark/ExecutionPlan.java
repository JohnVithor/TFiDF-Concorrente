package jv.microbenchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import jv.utils.*;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@State(Scope.Benchmark)
public class ExecutionPlan {
    @Param({"devel_100_000_id"})
//    @Param({"train_id"})
//    @Param({"devel_1_000_id", "devel_10_000_id", "devel_100_000_id", "test_id", "train_id"})
//    @Param({"devel_100_000_id", "train_id"})
    public String dataset;
//    @Param({"foreach_java", "foreach_apache", "stream_java", "stream_apache"})
//    @Param({"foreach_java", "foreach_apache"})
    @Param({"foreach_java"})
    public String stringManipulation;
    public UtilInterface util;
    public String stop_words_path = "datasets/stopwords.txt";
    public Path input_path;
    public Set<String> stopwords;
    public Map<String, Long> count;
    public AtomicInteger n_docs;
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
        n_docs = new AtomicInteger(0);
        // preparação para a segunda etapa do algoritmo
        UtilInterface util = new ForEachApacheUtil();
        stopwords = util.load_stop_words(stop_words_path);
        ObjectMapper objectMapper = new ObjectMapper();
        MapType type = objectMapper
                .getTypeFactory()
                .constructMapType(Map.class, String.class, Long.class);
        try {
            count = objectMapper.readValue(new File(dataset+".json"), type);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
