package jv.records;

import java.util.List;

public record TFiDFInfo(long vocabulary_size,
                        List<String> most_frequent_terms,
                        Long most_frequent_term_count,
                        Long n_docs,
                        List<Data> highest_tfidfs,
                        List<Data> lowest_tfidfs) {
}
