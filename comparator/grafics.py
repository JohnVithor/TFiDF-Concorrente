# grafics.py

import pandas as pd
import seaborn as sns

dados = pd.read_csv("../results.csv")

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runners.runner.")
dados["Class"] = dados.Benchmark.str.split("Runner.").str[0]
dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados.dropna(subset = ['Score'], inplace=True) 
replace_dataset = {"devel":"45.2 MB", "test":"177.9 MB", "train":"1.6 GB"}
replace_string = {"foreach_java":"Java", "foreach_apache":"Apache"}
dados["Dataset"] = dados["Param: dataset"].replace(replace_dataset)
dados["String"] = dados["Param: stringManipulation"].replace(replace_string)
dados.drop(["Param: dataset", "Param: stringManipulation"], axis=1, inplace=True)
alvos=set(["compute_df", "compute_tfidf"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="String",
    x="Dataset", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
    # order=['BasicSerial', 'ThreadConcurrent']
)

alvos=set(["compute_df:·gc.alloc.rate.norm", "compute_tfidf:·gc.alloc.rate.norm"])
g = sns.catplot(
    data=dados[dados["Benchmark"].isin(alvos)], kind="bar",
    col="Benchmark", row="String",
    x="Dataset", y="Score", hue="Class",
    ci="sd", palette="dark", alpha=.6, height=6, sharey=True,
    # order=['BasicSerial', 'ThreadConcurrent']
)
