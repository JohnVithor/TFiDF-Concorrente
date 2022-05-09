package jv.records;

public record TFiDFInfo(long vocabulary_size,
                        String most_frequent_term,
                        String less_frequent_term,
                        long biggest_document,
                        long smallest_document,
                        Data highest_tfidf,
                        Data lowest_tfidf) {}
