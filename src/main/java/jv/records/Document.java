package jv.records;

import java.util.Map;

public record Document(int id, Map<String, Long> counts, long n_terms) {}
