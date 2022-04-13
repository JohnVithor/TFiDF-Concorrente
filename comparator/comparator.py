import pandas as pd

file='devel_1_000'

serial = pd.read_parquet("results_serial/"+file+"_tfidf_results.parquet")
serial.sort_values(['doc_id', 'term'], inplace=True)
serial.reset_index(drop=True, inplace=True)

concurrent = pd.read_parquet("results_concurrent/"+file+"_tfidf_results.parquet")
concurrent.sort_values(['doc_id', 'term'], inplace=True)
concurrent.reset_index(drop=True, inplace=True)

print(serial.head())

print(concurrent.head())

print(serial.equals(concurrent))