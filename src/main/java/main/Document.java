package main;

import java.util.*;

public class Document {
	static Set<String> stopwords = new HashSet<>();
	static List<String> id_token_vocabulary = new ArrayList<>();
	static final HashMap<String, Integer> token_id_vocabulary = new HashMap<>();
	static int vocab_size = 0;
	private final String title;
	private final Map<Integer, Double> frequency_table = new HashMap<>();

	public Document(String title, String content) {
		this.title = title;
		String[] tokens = content
				.replaceAll("[^a-zA-Z0-9 ]", "")
				.toLowerCase()
				.split("\\s+");
		double numberOfTerms = tokens.length;
		for (String token: tokens) {
			if(token.length() == 0 || stopwords.contains(token))
			{
				continue;
			}
			int this_id;

			synchronized (token_id_vocabulary) {
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
		frequency_table.replaceAll((k, v) -> v / numberOfTerms);
	}

	public String getTitle() {
		return title;
	}

	public Map<Integer, Double> getFrequency_table() {
		return frequency_table;
	}

	public double calculateTermFrequency(Integer term_id) {
		return frequency_table.getOrDefault(term_id,0.0);
	}
}
