# grafics.py

import pandas as pd
import seaborn as sns

serial = pd.read_csv("./Throughput/SerialResults.csv")
serial["Class"] = "Serial"
concurrent = pd.read_csv("./Throughput/ConcurrentResults.csv")
concurrent["Class"] = "Raw Threads"

dados = pd.concat([serial, concurrent])

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runners.runner.")
dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados.dropna(subset = ['Score'], inplace=True) 
replace_string = {"foreach_java":"Java", "foreach_apache":"Apache"}
dados["String"] = dados["Param: stringManipulation"].replace(replace_string)
dados.drop(["Param: dataset", "Param: stringManipulation"], axis=1, inplace=True)
alvos=set(["compute_df", "compute_tfidf"])

g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark",
    x="String", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Serial X Concurrent (Throughput).png")

alvos=set(["compute_df:·gc.alloc.rate.norm", "compute_tfidf:·gc.alloc.rate.norm"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark",
    x="String", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Serial X Concurrent (GC).png")
