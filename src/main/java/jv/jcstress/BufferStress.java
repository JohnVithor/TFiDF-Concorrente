package jv.jcstress;

import org.openjdk.jcstress.annotations.*;

//@JCStressTest
@Description("Teste de buffer")
@Outcome(id="D, D", expect = Expect.ACCEPTABLE, desc = "")
public class BufferStress {
    @Actor
    public void actor1(BufferState state) {
    }

    @Actor
    public void actor2(BufferState state) {

    }
}