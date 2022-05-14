package jv.collectors;

import java.util.HashSet;
import java.util.Set;

public class MaxTermCount {

    private long max_count = 0L;
    private Set<String> terms = new HashSet<>();

    public MaxTermCount() {
    }

    public long getMax_count() {
        return max_count;
    }

    public void setMax_count(long max_count) {
        this.max_count = max_count;
    }

    public Set<String> getTerms() {
        return terms;
    }

    public void setTerms(Set<String> terms) {
        this.terms = terms;
    }
}
