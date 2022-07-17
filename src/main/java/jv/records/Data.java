package jv.records;

import java.io.Serializable;

public record Data(String term, long doc_id, double value) implements Serializable {
}
