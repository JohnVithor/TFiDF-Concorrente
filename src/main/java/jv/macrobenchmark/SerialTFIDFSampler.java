package jv.macrobenchmark;

import jv.records.TFiDFInfo;
import jv.tfidf.TFiDFInterface;
import jv.tfidf.naive.Concurrent;
import jv.tfidf.naive.Serial;
import jv.utils.ForEachApacheUtil;
import jv.utils.UtilInterface;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.io.IOException;
import java.nio.file.Path;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class SerialTFIDFSampler extends AbstractJavaSamplerClient implements Serializable {

    Set<String> stopworlds = null;
    UtilInterface util = new ForEachApacheUtil();

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        SampleResult result = new SampleResult();
        result.setSampleLabel("Test Sample");

        String corpus_path_str = javaSamplerContext.getParameter("corpus_path");
        String tfidf_str = javaSamplerContext.getParameter("TFiDF");
        boolean stopwords_flag = Boolean.parseBoolean(javaSamplerContext.getParameter("stopwords"));
        int n_threads = Integer.parseInt(javaSamplerContext.getParameter("n_threads"));
        int buffer_size = Integer.parseInt(javaSamplerContext.getParameter("buffer_size"));

        Set<String> selected_stopwords = new HashSet<>();
        if (stopwords_flag){
            selected_stopwords = stopworlds;
        }

        Path corpus_path = Path.of(corpus_path_str);
        TFiDFInfo expected_info = getExpectedInfo(corpus_path);
        TFiDFInterface tfidf = null;

        switch (tfidf_str) {
            case "Naive Serial" -> tfidf = new Serial(selected_stopwords, util, corpus_path);
            case "Consumer-Producer Threads" ->
                    tfidf = new Concurrent(selected_stopwords, util, corpus_path, n_threads, buffer_size);
            default -> {
                result.sampleStart();
                result.sampleEnd();
                result.setResponseCode("200");
                result.setResponseMessage("OK");
                result.setSuccessful(true);
                return result;
            }
        }

        result.sampleStart();
        try {
            tfidf.compute();
            if(tfidf.results().equals(expected_info)) {
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
        } catch (IOException e) {
            result.sampleEnd();
            result.setResponseCode("500");
            result.setResponseMessage(e.getMessage());
            result.setSuccessful(false);
        }
        return result;
    }

    TFiDFInfo getExpectedInfo(Path corpus_path) {
        return null;
    }

    @Override public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("corpus_path","");
        defaultParameters.addArgument("TFiDF","Naive Serial");
        defaultParameters.addArgument("stopwords","True");
        defaultParameters.addArgument("n_threads","2");
        defaultParameters.addArgument("buffer_size","1000");
        return defaultParameters;
    }
}
