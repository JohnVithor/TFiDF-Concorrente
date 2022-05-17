package jv.stresstest;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.CCCCCCCC_Result;

//@JCStressTest
//@Description("Testar o a ordem de leitura em relação a escrita")
//@Outcome(id="A, B, C, D, E, F, G, H", expect = Expect.ACCEPTABLE, desc = "Única sequencia permitida")
public class BufferChar1To1Test {

    @Actor
    public void reader(CharBufferState bufferState, CCCCCCCC_Result result) {
        try {
            result.r1 = bufferState.take();
            result.r2 = bufferState.take();
            result.r2 = bufferState.take();
            result.r3 = bufferState.take();
            result.r4 = bufferState.take();
            result.r5 = bufferState.take();
            result.r6 = bufferState.take();
            result.r7 = bufferState.take();
            result.r8 = bufferState.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Actor
    public void writer(CharBufferState bufferState) {
        try {
            bufferState.put('A');
            bufferState.put('B');
            bufferState.put('C');
            bufferState.put('D');
            bufferState.put('E');
            bufferState.put('F');
            bufferState.put('G');
            bufferState.put('H');
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
