# grafics.py

import pandas as pd
import seaborn as sns

g1gc = pd.read_csv("./Throughput/G1_Concurrent_Results.csv")
g1gc["GC"] = "G1GC"
zgc = pd.read_csv("./Throughput/Z_Concurrent_Results.csv")
zgc["GC"] = "ZGC"
shenandoahgc = pd.read_csv("./Throughput/Shenandoah_Concurrent_Results.csv")
shenandoahgc["GC"] = "ShenandoahGC"

dados = pd.concat([g1gc, zgc, shenandoahgc])

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runner.")
dados["Class"] = dados.Benchmark.str.split(".").str[1]
dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados["Threads"] = dados["Param: n_threads"].astype(int)
dados.dropna(subset = ['Score'], inplace=True)
dados.drop(["Param: dataset",
            "Param: stringManipulation",
            "Param: n_threads"], axis=1, inplace=True)
alvos=set(["compute_df", "compute_tfidf"])

g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="Class",
    x="GC", y="Score", hue="Threads",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Concurrent GCs (Throughput).png")

alvos=set(["compute_df:·gc.alloc.rate.norm", "compute_tfidf:·gc.alloc.rate.norm"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="Class",
    x="GC", y="Score", hue="Threads",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Concurrent GCs (GC).png")
