package jv.jcstress;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.IIII_Result;

@JCStressTest
@Description("Teste de buffer")
@Outcome(id="1, 2, 3, 4", expect = Expect.ACCEPTABLE, desc = "t1 t1 t2 t2")
@Outcome(id="3, 4, 1, 2", expect = Expect.ACCEPTABLE, desc = "t2 t2 t1 t1")
@Outcome(id="1, 3, 2, 4", expect = Expect.ACCEPTABLE, desc = "t1 t2 t1 t2")
@Outcome(id="2, 4, 1, 3", expect = Expect.ACCEPTABLE, desc = "t2 t1 t2 t1")
@Outcome(id="1, 4, 2, 3", expect = Expect.ACCEPTABLE, desc = "t1 t2 t2 t1")
@Outcome(id="2, 3, 1, 4", expect = Expect.ACCEPTABLE, desc = "t2 t1 t1 t2")
public class BufferStress {
    @Actor
    void writer(BufferState state) {
        try {
            state.put("1");
            state.put("2");
            state.put("3");
            state.put("4");
        } catch (Exception e){
            System.err.println(e.getMessage());
        }
    }

    @Actor
    public void reader1(BufferState state, IIII_Result result) {
        try {
            result.r1 = Integer.parseInt(state.take());
            result.r2 = Integer.parseInt(state.take());
        } catch (Exception e){
            System.err.println(e.getMessage());
        }
    }

    @Actor
    public void reader2(BufferState state, IIII_Result result) {
        try {
            result.r3 = Integer.parseInt(state.take());
            result.r4 = Integer.parseInt(state.take());
        } catch (Exception e){
            System.err.println(e.getMessage());
        }
    }
}