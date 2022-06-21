package jv.tfidf.forkjoin;

import jv.records.Data;
import jv.tfidf.naive.threads.ConsumerThreadDF;
import jv.tfidf.naive.threads.ConsumerThreadTFiDF;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public class JoinTFiDFTask extends RecursiveTask<Pair<List<Data>, List<Data>>> {

    private final List<ConsumerThreadTFiDF> threads;

    public JoinTFiDFTask(List<ConsumerThreadTFiDF> threads) {
        this.threads = threads;
    }

    @Override
    protected Pair<List<Data>, List<Data>> compute() {
        try {
            if (threads.size() == 1) {
                threads.get(0).join();
                return Pair.of(threads.get(0).getData_high(),
                        threads.get(0).getData_low());
            } else if (threads.size() == 2) {
                threads.get(0).join();
                List<Data> highest_tfidf = threads.get(0).getData_high();
                List<Data> lowest_tfidf = threads.get(0).getData_low();
                threads.get(1).join();
                if (threads.get(1).getHtfidf() > threads.get(0).getHtfidf()) {
                    highest_tfidf = threads.get(1).getData_high();
                } else if (threads.get(1).getHtfidf() == threads.get(0).getHtfidf()) {
                    highest_tfidf.addAll(threads.get(1).getData_high());
                }
                if (threads.get(1).getLtfidf() < threads.get(0).getLtfidf()) {
                    lowest_tfidf = threads.get(1).getData_low();
                } else if (threads.get(1).getLtfidf() == threads.get(0).getLtfidf()) {
                    lowest_tfidf.addAll(threads.get(1).getData_low());
                }
                return Pair.of(highest_tfidf, lowest_tfidf);
            } else {
                int mid = threads.size() / 2;
                JoinTFiDFTask t1 = new JoinTFiDFTask(threads.subList(0, mid));
                JoinTFiDFTask t2 = new JoinTFiDFTask(threads.subList(mid, threads.size()));
                t1.fork();
                Pair<List<Data>, List<Data>> p1 = t2.compute();
                List<Data> highest_tfidf = p1.getKey();
                List<Data> lowest_tfidf = p1.getValue();
                Pair<List<Data>, List<Data>> p2 = t1.join();
                List<Data> highest_tfidf_2 = p2.getKey();
                List<Data> lowest_tfidf_2 = p2.getValue();
                if (highest_tfidf_2.get(0).value() > highest_tfidf.get(0).value()) {
                    highest_tfidf = highest_tfidf_2;
                } else if (highest_tfidf_2.get(0).value() == highest_tfidf.get(0).value()) {
                    highest_tfidf.addAll(highest_tfidf_2);
                }
                if (lowest_tfidf_2.get(0).value() < lowest_tfidf.get(0).value()) {
                    lowest_tfidf = lowest_tfidf_2;
                } else if (lowest_tfidf_2.get(0).value() == lowest_tfidf.get(0).value()) {
                    lowest_tfidf.addAll(lowest_tfidf_2);
                }
                return Pair.of(highest_tfidf, lowest_tfidf);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
