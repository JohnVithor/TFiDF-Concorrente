package jv.microbenchmark;

import jv.microbenchmark.runners.tfidf.naive.NaiveSerialRunner;
import jv.microbenchmark.runners.tfidf.naive.ThreadConcurrentRunner;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Microbenchmark {
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(NaiveSerialRunner.class.getSimpleName())
                .include(ThreadConcurrentRunner.class.getSimpleName())
//                .include(StreamSerialRunner.class.getSimpleName())
//                .include(StreamConcurrentRunner.class.getSimpleName())
                .shouldDoGC(true)
                .addProfiler(GCProfiler.class)
//                .addProfiler(StackProfiler.class)
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .jvmArgs("-server"
                        , "-Xms2048m"
                        , "-Xmx2048m"
//                        ,"-XX:+UseSerialGC"
//                        ,"-XX:+UseParallelGC"
//                        ,"-XX:+UseConcMarkSweepGC"
//                        ,"-XX:+UseG1GC"
//                        ,"-XX:+UseStringDeduplication"
//                        ,"-XX:+UnlockExperimentalVMOptions"
//                        ,"-XX:+UseZGC"
//                        ,"-XX:+UseShenandoahGC"
                )
                .result("results.csv")
                .resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt).run();
    }
}
