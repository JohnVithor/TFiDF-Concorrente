package jv.tfidf.stream.collectors;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MaxTermCountCollector implements Collector<Map.Entry<String, Long>, MaxTermCount, MaxTermCount> {
    @Override
    public Supplier<MaxTermCount> supplier() {
        return MaxTermCount::new;
    }

    @Override
    public BiConsumer<MaxTermCount, Map.Entry<String, Long>> accumulator() {
        return (maxTermCount, e) -> {
            if (e.getValue() > maxTermCount.getMax_count()) {
                maxTermCount.setMax_count(e.getValue());
                maxTermCount.getTerms().clear();
                maxTermCount.getTerms().add(e.getKey());
            } else if (e.getValue() == maxTermCount.getMax_count()) {
                maxTermCount.getTerms().add(e.getKey());
            }
        };
    }

    @Override
    public BinaryOperator<MaxTermCount> combiner() {
        return (maxTermCount1, maxTermCount2) -> {
            if (maxTermCount1.getMax_count() < maxTermCount2.getMax_count()) {
                return maxTermCount1;
            } else if (maxTermCount1.getMax_count() == maxTermCount2.getMax_count()) {
                maxTermCount1.getTerms().addAll(maxTermCount2.getTerms());
                return maxTermCount1;
            } else {
                return maxTermCount2;
            }
        };
    }

    @Override
    public Function<MaxTermCount, MaxTermCount> finisher() {
        return null;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH,
                Characteristics.CONCURRENT,
                Characteristics.UNORDERED);
    }
}
