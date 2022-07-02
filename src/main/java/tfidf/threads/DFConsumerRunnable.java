package tfidf.threads;

import utils.MyBuffer;
import utils.UtilInterface;

import java.util.HashMap;
import java.util.Set;

public class DFConsumerRunnable implements Runnable {

    private final MyBuffer<String> buffer;
    private final UtilInterface util;
    private final Set<String> stopwords;
    private final String endline;

    private final HashMap<String, Long> count;

    public DFConsumerRunnable(MyBuffer<String> buffer,
                              UtilInterface util,
                              Set<String> stopwords,
                              String endline) {
        this.buffer = buffer;
        this.util = util;
        this.stopwords = stopwords;
        this.endline = endline;
        this.count = new HashMap<>();
    }

    @Override
    public void run() {
        try {
            while (true) {
                String line = buffer.take();
                if (line == endline) {
                    return;
                }
                for (String term : util.setOfTerms(line, stopwords)) {
                    count.put(term, count.getOrDefault(term, 0L) + 1L);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, Long> getCount() {
        return count;
    }
}
