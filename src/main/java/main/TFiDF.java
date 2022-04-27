package main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public interface TFiDF {
    default ExecutionData run(String target, boolean write) throws InterruptedException, IOException {
        setup(target);
        Instant start = Instant.now();
        firstHalf();
        Instant mid = Instant.now();
        if(write){
            secondHalfWriting();
        } else {
            secondHalfNotWriting();
        }
        Instant end = Instant.now();
        return new ExecutionData(
                Duration.between(start, mid),
                Duration.between(mid, end),
                Duration.between(start, end)
        );
    }
    void setup(String target);
    void firstHalf() throws InterruptedException;
    void secondHalfWriting() throws IOException;
    void secondHalfNotWriting();
}
