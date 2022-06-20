package jv.tfidf.forkjoin;

import jv.tfidf.naive.threads.ConsumerThreadDF;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public class JoinHashTask extends RecursiveTask<HashMap<String, Long>> {

    private final List<ConsumerThreadDF> threads;

    JoinHashTask(List<ConsumerThreadDF> threads) {
        this.threads = threads;
    }

    @Override
    protected HashMap<String, Long> compute() {
        try {
            if (threads.size() == 1) {
                threads.get(0).join();
                return threads.get(0).getCount();
            } else if (threads.size() == 2) {
                threads.get(0).join();
                HashMap<String, Long> r = threads.get(0).getCount();
                threads.get(1).join();
                for (Map.Entry<String, Long> pair : threads.get(1).getCount().entrySet()) {
                    r.put(pair.getKey(), r.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
                return r;
            } else {
                int mid = threads.size() / 2;
                JoinHashTask t1 = new JoinHashTask(threads.subList(0, mid));
                JoinHashTask t2 = new JoinHashTask(threads.subList(mid, threads.size()));
                t1.fork();
                HashMap<String, Long> r = t2.compute();
                for (Map.Entry<String, Long> pair : t1.join().entrySet()) {
                    r.put(pair.getKey(), r.getOrDefault(pair.getKey(), 0L) + pair.getValue());
                }
                return r;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
