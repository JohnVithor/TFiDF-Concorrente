package jv.stresstest;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.JJJ_Result;
import org.openjdk.jcstress.infra.results.JJ_Result;

//@JCStressTest
//@Description("Testar o put do buffer (duas escritas e duas leituras)")
//@Outcome(id="2, 0, 0", expect = Expect.ACCEPTABLE, desc = "escreveu mas não leu ainda")
//@Outcome(id="2, 1, 0", expect = Expect.ACCEPTABLE, desc = "escreveu mas não leu ainda")
//@Outcome(id="2, 2, 0", expect = Expect.ACCEPTABLE, desc = "escreveu mas não leu ainda")
//@Outcome(id="0, 0, 0", expect = Expect.ACCEPTABLE, desc = "escreveu e já leu tudo")
public class BufferPut4Read2Test {

    @Actor
    public void reader(BufferState bufferState, JJJ_Result result) {
        try {
            bufferState.take();
            bufferState.take();
            result.r2 = bufferState.size();
            bufferState.take();
            bufferState.take();
            result.r3 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void writer(BufferState bufferState, JJ_Result result) {
        try {
            bufferState.put("linha1");
            bufferState.put("linha2");
            bufferState.put("linha3");
            bufferState.put("linha4");
            result.r1 = bufferState.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
