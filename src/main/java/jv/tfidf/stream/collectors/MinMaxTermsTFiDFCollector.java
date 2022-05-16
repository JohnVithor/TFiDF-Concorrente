package jv.tfidf.stream.collectors;

import jv.records.Data;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class MinMaxTermsTFiDFCollector implements Collector<Data, MinMaxTermsTFiDF, MinMaxTermsTFiDF> {
    @Override
    public Supplier<MinMaxTermsTFiDF> supplier() {
        return MinMaxTermsTFiDF::new;
    }

    @Override
    public BiConsumer<MinMaxTermsTFiDF, Data> accumulator() {
        return (minMaxTermsTFiDF, data) -> {
            if (data.value() > minMaxTermsTFiDF.getHighest_tfidf()) {
                minMaxTermsTFiDF.setHighest_tfidf(data.value());
                minMaxTermsTFiDF.getHighest_tfidfs().clear();
                minMaxTermsTFiDF.getHighest_tfidfs().add(data);
            } else if (data.value() == minMaxTermsTFiDF.getHighest_tfidf()) {
                minMaxTermsTFiDF.getHighest_tfidfs().add(data);
            }
            if (data.value() < minMaxTermsTFiDF.getLowest_tfidf()) {
                minMaxTermsTFiDF.setLowest_tfidf(data.value());
                minMaxTermsTFiDF.getLowest_tfidfs().clear();
                minMaxTermsTFiDF.getLowest_tfidfs().add(data);
            } else if (data.value() == minMaxTermsTFiDF.getLowest_tfidf()) {
                minMaxTermsTFiDF.getLowest_tfidfs().add(data);
            }
        };
    }

    @Override
    public BinaryOperator<MinMaxTermsTFiDF> combiner() {
        return (MinMaxTermsTFiDF1, MinMaxTermsTFiDF2) ->
        {
            MinMaxTermsTFiDF result = new MinMaxTermsTFiDF();
            if (MinMaxTermsTFiDF1.getHighest_tfidf() > MinMaxTermsTFiDF2.getHighest_tfidf()) {
                result.getHighest_tfidfs().addAll(MinMaxTermsTFiDF1.getHighest_tfidfs());
            } else if (MinMaxTermsTFiDF1.getHighest_tfidf() == MinMaxTermsTFiDF2.getHighest_tfidf()) {
                result.getHighest_tfidfs().addAll(MinMaxTermsTFiDF1.getHighest_tfidfs());
                result.getHighest_tfidfs().addAll(MinMaxTermsTFiDF2.getHighest_tfidfs());
            } else {
                result.getHighest_tfidfs().addAll(MinMaxTermsTFiDF2.getHighest_tfidfs());
            }
            if (MinMaxTermsTFiDF1.getLowest_tfidf() < MinMaxTermsTFiDF2.getLowest_tfidf()) {
                result.getLowest_tfidfs().addAll(MinMaxTermsTFiDF1.getLowest_tfidfs());
            } else if (MinMaxTermsTFiDF1.getLowest_tfidf() == MinMaxTermsTFiDF2.getLowest_tfidf()) {
                result.getLowest_tfidfs().addAll(MinMaxTermsTFiDF1.getLowest_tfidfs());
                result.getLowest_tfidfs().addAll(MinMaxTermsTFiDF2.getLowest_tfidfs());
            } else {
                result.getLowest_tfidfs().addAll(MinMaxTermsTFiDF2.getLowest_tfidfs());
            }
            return result;
        };
    }

    @Override
    public Function<MinMaxTermsTFiDF, MinMaxTermsTFiDF> finisher() {
        return null;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.IDENTITY_FINISH,
                Characteristics.CONCURRENT,
                Characteristics.UNORDERED);
    }
}
