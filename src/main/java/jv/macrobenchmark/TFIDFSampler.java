package jv.macrobenchmark;

import jv.records.Data;
import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.atomic.Concurrent;
import jv.tfidf.executor.ProducerConsumerConcurrent;
import jv.tfidf.executor.SmallTasksConcurrent;
import jv.tfidf.naive.Serial;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class TFIDFSampler extends AbstractJavaSamplerClient {

    private final UtilInterface util = new ForEachApacheUtil();
    private final Set<String> stopworlds = util.load_stop_words("/home/johnvithor/UFRN/Concorrente/TFiDF-Concorrente/stopwords.txt");

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("Test Sample");

        String corpus_name = javaSamplerContext.getParameter("corpus_name");
        String tfidf_str = javaSamplerContext.getParameter("TFiDF");
        boolean stopwords_flag = Boolean.parseBoolean(javaSamplerContext.getParameter("stopwords"));
        int n_threads = Integer.parseInt(javaSamplerContext.getParameter("n_threads"));
        int buffer_size = Integer.parseInt(javaSamplerContext.getParameter("buffer_size"));

        Set<String> selected_stopwords = new HashSet<>();
        if (stopwords_flag){
            selected_stopwords = stopworlds;
        }

        Path corpus_path = Path.of("/home/johnvithor/UFRN/Concorrente/TFiDF-Concorrente/datasets/"+corpus_name+".csv");
        TFiDFInfo expected_info = getExpectedInfo(corpus_name);
        if (expected_info == null) {
            result.sampleStart();
            result.sampleEnd();
            result.setResponseCode("500");
            result.setResponseMessage("corpus not supported");
            result.setSuccessful(false);
            return result;
        }
        TFiDFInterface tfidf;

        switch (tfidf_str) {
            case "Naive Serial" ->
                    tfidf = new Serial(selected_stopwords, util, corpus_path);
            case "Consumer-Producer Threads" ->
                    tfidf = new jv.tfidf.naive.Concurrent(selected_stopwords, util, corpus_path, n_threads, buffer_size);
            case "Small Task" ->
                    tfidf = new SmallTasksConcurrent(selected_stopwords, util, corpus_path, n_threads);
            case "Big Task" ->
                    tfidf = new ProducerConsumerConcurrent(selected_stopwords, util, corpus_path, n_threads, buffer_size);
            case "Atomic Concurrent" ->
                    tfidf = new jv.tfidf.atomic.Concurrent(selected_stopwords, util, corpus_path, n_threads, buffer_size);
            case "ForkJoin Concurrent" ->
                    tfidf = new jv.tfidf.forkjoin.Concurrent(selected_stopwords, util, corpus_path, n_threads, buffer_size);
            default -> {
                result.sampleStart();
                result.sampleEnd();
                result.setResponseCode("500");
                result.setResponseMessage("TFiDF approach not known");
                result.setSuccessful(false);
                return result;
            }
        }

        result.sampleStart();
        tfidf.compute();
        TFiDFInfo r = tfidf.results();
        String responseData = "{\"actual\"=" + r + ",\n" +
                "\"expected\"=" + expected_info + "}";
        result.setResponseData(responseData, "UTF-8");
        if(r.equals(expected_info)) {
            result.sampleEnd();
            result.setResponseCode("200");
            result.setResponseMessage("OK");
            result.setSuccessful(true);
        } else {
            result.sampleEnd();
            result.setResponseCode("500");
            result.setResponseMessage("NOK");
            result.setSuccessful(false);
        }
        return result;
    }

    TFiDFInfo getExpectedInfo(String corpus_name) {
        return switch (corpus_name) {
            case "train" -> new TFiDFInfo(
                    2328897L,
                    List.of("book"),
                    902838L,
                    3600000L,
                    List.of(new Data("morethink", 135468 , 14.40329722286639200)),
                    List.of(new Data("book"     , 817017 , 0.011430958587727554),
                            new Data("book"     , 2778979, 0.011430958587727554))
            );
            case "test" -> new TFiDFInfo(
                    491669L,
                    List.of("book"),
                    100047L,
                    400000L,
                    List.of(new Data("qqq" , 18753  , 8.616051279197771000)),
                    List.of(new Data("book", 361312, 0.0115485372627941260))
            );
            case "devel" -> new TFiDFInfo(
                    180809L,
                    List.of("book"),
                    27747L,
                    100000L,
                    List.of(new Data("stopplease", 42525, 9.045870008190894000)),
                    List.of(new Data("book"      , 74354, 0.013785402792987728))
            );
            default -> null;
        };
    }

    @Override public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("corpus_name","");
        defaultParameters.addArgument("TFiDF","Naive Serial");
        defaultParameters.addArgument("stopwords","True");
        defaultParameters.addArgument("n_threads","2");
        defaultParameters.addArgument("buffer_size","1000");
        return defaultParameters;
    }
}

