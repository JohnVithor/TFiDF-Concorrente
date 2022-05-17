package jv.stresstest;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.CC_Result;

@JCStressTest
@Description("Testar o put do buffer (duas escritas e duas leituras)")
@Outcome(id="A, B", expect = Expect.ACCEPTABLE, desc = "escreveu, escreveu, size1, leu, leu, size2")
@Outcome(id="B, A", expect = Expect.ACCEPTABLE, desc = "escreveu, leu, escreveu, size1, leu, size2")
public class BufferPut2Read2Test2 {

    @Actor
    public void reader1(CharBufferState bufferState, CC_Result result) {
        try {
           result.r1 = bufferState.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void reader2(CharBufferState bufferState, CC_Result result) {
        try {
            result.r2 = bufferState.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void writer(CharBufferState bufferState) {
        try {
            bufferState.put('A');
            bufferState.put('B');
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
