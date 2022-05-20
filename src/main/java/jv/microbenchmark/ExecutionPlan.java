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

@State(Scope.Benchmark)
public class ExecutionPlan {
    @Param({"devel"})
//    @Param({"test"})
//    @Param({"train"})
    public String dataset;
    //    @Param({"foreach_java", "foreach_apache"})
    @Param({"foreach_apache"})
    public String stringManipulation;

    @Param({"2", "4"})
    public int n_threads;

    @Param({"1000"})
    public int buffer_size;

    public UtilInterface util;
    public String stop_words_path = "stopwords.txt";
    public Path corpus_path;
    public Set<String> stopwords;
    public Map<String, Long> count;
    public long n_docs;
    public long modifiable_n_docs;

    @Setup(Level.Iteration)
    public void setUp() {
        corpus_path = Path.of("datasets/" + dataset + ".csv");
        switch (dataset) {
            case "devel" -> n_docs = 100000;
            case "test" -> n_docs = 400000;
            case "train" -> n_docs = 3600000;
        }
        if ("foreach_java".equals(stringManipulation)) {
            util = new ForEachJavaUtil();
        } else {
            util = new ForEachApacheUtil();
        }
        // preparação para a segunda etapa do algoritmo
        UtilInterface util = new ForEachApacheUtil();
        stopwords = util.load_stop_words(stop_words_path);

        ObjectMapper objectMapper = new ObjectMapper();
        MapType type = objectMapper
                .getTypeFactory()
                .constructMapType(Map.class, String.class, Long.class);
        try {
            count = objectMapper.readValue(new File("datasets/" + dataset+".json"), type);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
