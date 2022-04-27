package main.microbenchmark;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

public class Runner {
    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void serial(ExecutionPlan plan, Blackhole blackhole) throws IOException, InterruptedException {
        blackhole.consume(plan.serial.run(plan.param, plan.write));
    }

    @Fork(value = 1)
    @Measurement(iterations = 5)
    @Warmup(iterations = 5)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void concurrent(ExecutionPlan plan, Blackhole blackhole) throws IOException, InterruptedException {
        blackhole.consume(plan.concurrent.run(plan.param , plan.write));
    }
}
