# grafics.py

import pandas as pd
import seaborn as sns

concurrent = pd.read_csv("./Throughput/ConcurrentResults.csv")
concurrent["Class"] = "Raw Threads"
executor = pd.read_csv("./Throughput/ExecutorResults.csv")
executor["Class"] = "Executor"

dados = pd.concat([concurrent, executor])

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runners.runner.")
dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados.dropna(subset = ['Score'], inplace=True) 
replace_string = {"foreach_java":"Java", "foreach_apache":"Apache"}
dados["String"] = dados["Param: stringManipulation"].replace(replace_string)
dados.drop(["Param: dataset", "Param: stringManipulation"], axis=1, inplace=True)
alvos=set(["compute_df", "compute_tfidf"])

# Apens versão com Apache
dados = dados[dados["String"] == "Apache"]

g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark",
    x="String", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Concurrent X Executor (Throughput).png")

alvos=set(["compute_df:·gc.alloc.rate.norm", "compute_tfidf:·gc.alloc.rate.norm"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark",
    x="String", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Concurrent X Executor (GC).png")
