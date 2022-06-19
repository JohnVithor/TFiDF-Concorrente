# grafics.py

import pandas as pd
import seaborn as sns

sg1gc = pd.read_csv("./Throughput/G1_SerialOnly_String_Results.csv")
sg1gc["GC"] = "G1GC"
szgc = pd.read_csv("./Throughput/Z_SerialOnly_String_Results.csv")
szgc["GC"] = "ZGC"
sshenandoahgc = pd.read_csv("./Throughput/Shenandoah_SerialOnly_String_Results.csv")
sshenandoahgc["GC"] = "ShenandoahGC"

dados = pd.concat([sg1gc, szgc, sshenandoahgc])

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
    x="String", y="Score", hue="GC",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Serial by Throughput.png")

alvos=set(["compute_df:·gc.time", "compute_tfidf:·gc.time"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark",
    x="String", y="Score", hue="GC",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
)
g.savefig("imgs/Serial by GC time.png")
