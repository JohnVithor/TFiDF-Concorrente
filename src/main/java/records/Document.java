package records;

import java.util.Map;

public record Document(long id, Map<String, Long> counts, long n_terms) {
}
