package jv.stresstest;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.JJ_Result;

//@JCStressTest
//@Description("Testar o put do buffer (uma escrita e uma leitura)")
//@Outcome(id="1, 0", expect = Expect.ACCEPTABLE, desc = "escreveu, size1, leu, size2")
//@Outcome(id="0, 0", expect = Expect.ACCEPTABLE, desc = "escreveu, leu, size2, size1")
public class BufferPut1Read1Test {

    @Actor
    public void reader(BufferState bufferState, JJ_Result result) {
        try {
            bufferState.take();
            result.r2 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void writer(BufferState bufferState, JJ_Result result) {
        try {
            bufferState.put("linha");
            result.r1 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
