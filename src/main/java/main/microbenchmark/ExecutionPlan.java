package main.microbenchmark;

import main.TFiDF;
import org.openjdk.jmh.annotations.*;

@State(Scope.Benchmark)
public class ExecutionPlan {

//    @Param({ "naive", "optimized", "non-optimized"})
    @Param({"naive"})
    public String alg_version;
    @Param({"false"})
    public boolean write;
    public TFiDF serial;
    public TFiDF concurrent;
    @Param({"devel_1_000_id", "devel_10_000_id", "devel_100_000_id"})
    public String param;
    @Setup(Level.Invocation)
    public void setUp() throws Exception {
        switch (alg_version) {
            case "naive":
                serial = new main.naive.Serial();
                concurrent = new main.naive.Concurrent();
                break;
            case "optimized":
                break;
            case "non-optimized":
                break;
            default:
                throw new Exception("Versão Inválida do alg_version: " + alg_version);
        }
    }
}
