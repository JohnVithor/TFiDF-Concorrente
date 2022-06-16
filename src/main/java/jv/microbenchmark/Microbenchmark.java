package jv.microbenchmark;

import jv.microbenchmark.runner.atomic.AtomicConcurrentRunner;
import jv.microbenchmark.runner.executor.ExecutorConcurrentRunner;
import jv.microbenchmark.runner.naive.NaiveSerialRunner;
import jv.microbenchmark.runner.naive.ThreadConcurrentRunner;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.StackProfiler;
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
                .include(ExecutorConcurrentRunner.class.getSimpleName())
                .include(AtomicConcurrentRunner.class.getSimpleName())
                .shouldDoGC(true)
                .mode(Mode.Throughput)
                .addProfiler(GCProfiler.class)
                .addProfiler(StackProfiler.class)
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .jvmArgs("-server"
                        , "-Xms2g"
                        , "-Xmx2g"
//                        ,"-XX:+UseSerialGC"
//                        ,"-XX:+UseParallelGC"
//                        ,"-XX:+UseConcMarkSweepGC"
                        ,"-XX:+UseG1GC"
//                        ,"-XX:+UseStringDeduplication"
//                        ,"-XX:+UnlockExperimentalVMOptions"
//                        ,"-XX:+UseZGC"
//                        ,"-XX:+UseShenandoahGC"
                )
                .result("Sresults.csv")
                .resultFormat(ResultFormatType.CSV)
                .build();
        new Runner(opt).run();
    }
}
