package jv.tfidf.future.threads;

import jv.utils.MyBuffer;
import jv.utils.UtilInterface;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class ConsumerThreadDF extends Thread {

    private final MyBuffer<String> buffer;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final String endline;

    private final HashMap<String, Long> count;

    private final CompletableFuture<HashMap<String, Long>> result;

    public ConsumerThreadDF(MyBuffer<String> buffer,
                            UtilInterface util,
                            Set<String> stopwords,
                            String endline,
                            CompletableFuture<HashMap<String, Long>> result) {
        this.buffer = buffer;
        this.util = util;
        this.stopwords = stopwords;
        this.endline = endline;
        this.count = new HashMap<>();
        this.result = result;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String line = buffer.take();
                if (line == endline) {
                    result.complete(count);
                    return;
                }
                for (String term : util.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L) + 1L);
                }
            }
        } catch (InterruptedException e) {
            result.completeExceptionally(e);
            throw new RuntimeException(e);
        }
    }
}
