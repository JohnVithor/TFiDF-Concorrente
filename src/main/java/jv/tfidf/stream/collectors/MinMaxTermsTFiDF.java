package jv.tfidf.stream.collectors;

import jv.records.Data;

import java.util.HashSet;
import java.util.Set;

public class MinMaxTermsTFiDF {
    private double highest_tfidf = 0.0;
    private double lowest_tfidf = Double.MAX_VALUE;
    private Set<Data> highest_tfidfs = new HashSet<>();
    private Set<Data> lowest_tfidfs = new HashSet<>();

    public MinMaxTermsTFiDF() {
    }

    public double getHighest_tfidf() {
        return highest_tfidf;
    }

    public void setHighest_tfidf(double highest_tfidf) {
        this.highest_tfidf = highest_tfidf;
    }

    public double getLowest_tfidf() {
        return lowest_tfidf;
    }

    public void setLowest_tfidf(double lowest_tfidf) {
        this.lowest_tfidf = lowest_tfidf;
    }

    public Set<Data> getHighest_tfidfs() {
        return highest_tfidfs;
    }

    public void setHighest_tfidfs(Set<Data> highest_tfidfs) {
        this.highest_tfidfs = highest_tfidfs;
    }

    public Set<Data> getLowest_tfidfs() {
        return lowest_tfidfs;
    }

    public void setLowest_tfidfs(Set<Data> lowest_tfidfs) {
        this.lowest_tfidfs = lowest_tfidfs;
    }
}
