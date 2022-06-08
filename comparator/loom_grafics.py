# grafics.py

import pandas as pd
import seaborn as sns

threads_c6 = pd.read_csv("./with-without-loom/results_6.csv")
threads_c6["Class"] = "Without Loom"
threads_c12 = pd.read_csv("./with-without-loom/results_12.csv")
threads_c12["Class"] = "Without Loom"
threads_c24 = pd.read_csv("./with-without-loom/results_24.csv")
threads_c24["Class"] = "Without Loom"
threads_v6 = pd.read_csv("./with-without-loom/loom_results_6.csv")
threads_v6["Class"] = "With Loom"
threads_v12 = pd.read_csv("./with-without-loom/loom_results_12.csv")
threads_v12["Class"] = "With Loom"
threads_v24 = pd.read_csv("./with-without-loom/loom_results_24.csv")
threads_v24["Class"] = "With Loom"
dados = pd.concat([threads_c6, threads_c12, threads_c24, threads_v6, threads_v12, threads_v24])

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runners.runner.")
# dados["Class"] = dados.Benchmark.str.split("Runner.").str[0]
dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados.dropna(subset = ['Score'], inplace=True)
replace_dataset = {"devel":"45.2 MB", "test":"177.9 MB", "train":"1.6 GB"}
replace_string = {"foreach_java":"Java", "foreach_apache":"Apache"}
dados["Dataset"] = dados["Param: dataset"].replace(replace_dataset)
dados["String"] = dados["Param: stringManipulation"].replace(replace_string)
dados["Threads"] = dados["Param: n_threads"].replace(replace_string)
dados.drop(["Param: dataset", "Param: stringManipulation"], axis=1, inplace=True)
alvos=set(["compute_df", "compute_tfidf"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="Dataset",
    x="Threads", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
    # order=['BasicSerial', 'ThreadConcurrent']
)
g.savefig("tfidf steps.png")

alvos=set(["compute_df:·gc.alloc.rate.norm", "compute_tfidf:·gc.alloc.rate.norm"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="Dataset",
    x="Threads", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
    # order=['BasicSerial', 'ThreadConcurrent']
)

g.savefig("gc use.png")
