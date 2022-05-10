package jv.records;

import java.util.List;

public record TFiDFInfo(long vocabulary_size,
                        List<String> most_frequent_terms,
                        Long most_frequent_term_count,
                        Long n_docs,
                        List<Long> biggest_documents,
                        Long biggest_document_count,
                        List<Long> smallest_documents,
                        Long smallest_document_count,
                        List<Data> highest_tfidfs,
                        List<Data> lowest_tfidfs) {}
