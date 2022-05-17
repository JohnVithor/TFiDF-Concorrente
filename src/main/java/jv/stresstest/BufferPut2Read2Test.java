package jv.stresstest;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.JJ_Result;

//@JCStressTest
//@Description("Testar o put do buffer (duas escritas e duas leituras)")
//@Outcome(id="2, 0", expect = Expect.ACCEPTABLE, desc = "escreveu, escreveu, size1, leu, leu, size2")
//@Outcome(id="1, 0", expect = Expect.ACCEPTABLE, desc = "escreveu, leu, escreveu, size1, leu, size2")
//@Outcome(id="0, 0", expect = Expect.ACCEPTABLE, desc = "escreveu, escreveu, leu, leu, size1, size2")
public class BufferPut2Read2Test {

    @Actor
    public void reader(BufferState bufferState, JJ_Result result) {
        try {
            bufferState.take();
            bufferState.take();
            result.r2 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void writer(BufferState bufferState, JJ_Result result) {
        try {
            bufferState.put("linha1");
            bufferState.put("linha2");
            result.r1 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
