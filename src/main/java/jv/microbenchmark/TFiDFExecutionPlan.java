package jv.microbenchmark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jv.utils.ForEachApacheUtil;
import jv.utils.ForEachJavaUtil;
import jv.utils.UtilInterface;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

@State(Scope.Benchmark)
public class TFiDFExecutionPlan {
//    @Param({"devel"})
    @Param({"test"})
//    @Param({"train"})
    public String dataset;

//    @Param({"foreach_java", "foreach_apache"})
    @Param({"foreach_apache"})
    public String stringManipulation;

    @Param({"4"})
    public int n_threads;

    @Param({"1000"})
    public int buffer_size;

    public UtilInterface util;
    public String stop_words_path = "stopwords.txt";
    public Path corpus_path;
    public Set<String> stopwords;
    public Map<String, Long> count;
    public long n_docs;

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

        Gson gson = new Gson();
        Type empMapType = new TypeToken<Map<String, Long>>() {}.getType();
        try {
            count = gson.fromJson(Files.readString(
                    Path.of("datasets/" + dataset + ".json")),
                    empMapType
            );
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
