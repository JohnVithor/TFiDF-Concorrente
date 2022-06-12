package jv.tfidf.atomic.callable;

import jv.utils.MyBuffer;
import jv.utils.UtilInterface;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public class TaskDFConsumer implements Callable<HashMap<String, Long>> {

    private final MyBuffer<String> buffer;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final String endline;
    private final AtomicLong n_docs;
    private final HashMap<String, Long> count;

    public TaskDFConsumer(MyBuffer<String> buffer,
                          UtilInterface util,
                          Set<String> stopwords,
                          String endline,
                          AtomicLong n_docs) {
        this.buffer = buffer;
        this.util = util;
        this.stopwords = stopwords;
        this.endline = endline;
        this.n_docs = n_docs;
        this.count = new HashMap<>();
    }

    @Override
    public HashMap<String, Long> call() {
        try {
            while (true) {
                String line = buffer.take();
                if (line == endline) {
                    return count;
                }
                for (String term : util.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L) + 1L);
                }
                n_docs.incrementAndGet();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
