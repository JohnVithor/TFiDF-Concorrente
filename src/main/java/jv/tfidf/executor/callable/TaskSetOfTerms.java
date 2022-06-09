package jv.tfidf.executor.callable;

import jv.utils.UtilInterface;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;

public class TaskSetOfTerms implements Callable<HashMap<String, Long>> {

    private final String line;
    private final UtilInterface util;
    private final Set<String> stopwords;

    public TaskSetOfTerms(String line,
                          UtilInterface util,
                          Set<String> stopwords
    ) {
        this.line = line;
        this.util = util;
        this.stopwords = stopwords;
    }

    @Override
    public HashMap<String, Long> call() throws Exception {
        HashMap<String, Long> count = new HashMap<>();
        for (String term : util.setOfTerms(line, stopwords)) {
            count.put(term, count.getOrDefault(term, 0L) + 1L);
        }
        return count;
    }
}
