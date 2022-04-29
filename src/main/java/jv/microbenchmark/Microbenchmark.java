package jv.microbenchmark;

import jv.microbenchmark.runners.BasicSerialRunner;
import jv.microbenchmark.runners.StreamConcurrentRunner;
import jv.microbenchmark.runners.StreamSerialRunner;
import jv.microbenchmark.runners.ThreadConcurrentRunner;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Microbenchmark {
    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BasicSerialRunner.class.getSimpleName())
                .include(ThreadConcurrentRunner.class.getSimpleName())
                .include(StreamSerialRunner.class.getSimpleName())
                .include(StreamConcurrentRunner.class.getSimpleName())
//                .include(OptimizedSerialRunner.class.getSimpleName())
//                .include(OptimizedConcurrentRunner.class.getSimpleName())
                .shouldDoGC(true)
                .addProfiler(GCProfiler.class)
                .addProfiler(StackProfiler.class)
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .jvmArgs("-server"
                        ,"-Xms2048m"
                        ,"-Xmx2048m"
//                        ,"-XX:+UnlockExperimentalVMOptions",
//                        ,"-XX:+UseZGC"
                )
                .result("all_results_zgc.csv")
                .resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt).run();
    }
}
