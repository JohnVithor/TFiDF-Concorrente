package jv.macrobenchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;


public class test extends AbstractJavaSamplerClient {

    @Override
    public void setupTest(JavaSamplerContext context){
        // TODO Auto-generated method stub

        super.setupTest(context);
    }
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("corpus_path","");
        defaultParameters.addArgument("TFiDF","Naive Serial");
        defaultParameters.addArgument("stopwords","True");
        defaultParameters.addArgument("n_threads","2");
        defaultParameters.addArgument("buffer_size","1000");
        return defaultParameters;
    }
    @Override
    public SampleResult runTest(JavaSamplerContext arg0) {
        // TODO Auto-generated method stub

        SampleResult result = new SampleResult();

        boolean success = true;

        result.sampleStart();

        // Write your test code here.

        //


        result.sampleEnd();

        result.setSuccessful(success);

        return result;

    }
    @Override
    public void teardownTest(JavaSamplerContext context){
        super.teardownTest(context);
    }
}
