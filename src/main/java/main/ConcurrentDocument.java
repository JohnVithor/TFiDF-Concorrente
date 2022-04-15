package main;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentDocument {
	static private final Object lock = new Object();
	static Set<String> stopwords = new HashSet<>();
	static List<String> id_token_vocabulary = new ArrayList<>();
	static final HashMap<String, Integer> token_id_vocabulary = new HashMap<>();
	static int vocab_size = 0;
	private final int id;
	private final String title;
	private final Map<Integer, Double> frequency_table = new HashMap<>();

	public ConcurrentDocument(int id, String title, String content) {
		this.id = id;
		this.title = title;
		AtomicInteger numberOfTerms = new AtomicInteger();
		Arrays.stream(content.replaceAll("[^a-zA-Z0-9 ]", "").split("\\s+"))
				.map(String::strip)
				.map(String::toLowerCase).forEach(token -> {
			if(token.length() != 0 && !stopwords.contains(token))
			{
				numberOfTerms.set(numberOfTerms.get()+1);
				int this_id;
				synchronized (lock) {
					this_id = token_id_vocabulary.getOrDefault(token, vocab_size);
					if (this_id == vocab_size) {
						token_id_vocabulary.put(token, vocab_size);
						id_token_vocabulary.add(token);
						++vocab_size;
					}
				}
				frequency_table.compute(this_id, (key, val)
						-> (val == null)
						? 1
						: val + 1);
			}
		});
		frequency_table.replaceAll((k, v) -> v / numberOfTerms.doubleValue());
	}

	public String getTitle() {
		return title;
	}

	public Map<Integer, Double> getFrequency_table() {
		return frequency_table;
	}

	public int getId() {
		return id;
	}

	public double calculateTermFrequency(Integer term_id) {
		return frequency_table.getOrDefault(term_id,0.0);
	}
}
