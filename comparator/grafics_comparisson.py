# grafics.py

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sg1gc = pd.read_csv("./Throughput/G1_Serial_Results.csv")
sg1gc["GC"] = "G1GC"
szgc = pd.read_csv("./Throughput/Z_Serial_Results.csv")
szgc["GC"] = "ZGC"
sshenandoahgc = pd.read_csv("./Throughput/Shenandoah_Serial_Results.csv")
sshenandoahgc["GC"] = "ShenandoahGC"

g1fjpool = pd.read_csv("./Throughput/G1_ForkJoin_Results.csv")
g1fjpool["GC"] = "G1GC"
zfjpool = pd.read_csv("./Throughput/Z_ForkJoin_Results.csv")
zfjpool["GC"] = "ZGC"
shfjpool = pd.read_csv("./Throughput/Shenandoah_ForkJoin_Results.csv")
shfjpool["GC"] = "ShenandoahGC"

g1stream = pd.read_csv("./Throughput/G1_Stream_Results.csv")
g1stream["GC"] = "G1GC"
zstream = pd.read_csv("./Throughput/Z_Stream_Results.csv")
zstream["GC"] = "ZGC"
shstream = pd.read_csv("./Throughput/Shenandoah_Stream_Results.csv")
shstream["GC"] = "ShenandoahGC"

cg1gc = pd.read_csv("./Throughput/G1_Concurrent_Results.csv")
cg1gc["GC"] = "G1GC"
czgc = pd.read_csv("./Throughput/Z_Concurrent_Results.csv")
czgc["GC"] = "ZGC"
cshenandoahgc = pd.read_csv("./Throughput/Shenandoah_Concurrent_Results.csv")
cshenandoahgc["GC"] = "ShenandoahGC"

dados = pd.concat([ sg1gc, szgc, sshenandoahgc,
                    cg1gc, czgc, cshenandoahgc,
                    g1fjpool, zfjpool, shfjpool,
                    g1stream,zstream,shstream])
dados.reset_index(inplace=True, drop=True)

dados.Score = dados.Score.str.replace(',', '.').astype(float)
dados.Benchmark = dados.Benchmark.str.removeprefix("jv.microbenchmark.runner.")
dados["Class"] = dados.Benchmark.str.split(".").str[1]

classes = [
    "NaiveSerialRunner",
    "ThreadConcurrentRunner",
    "ExecutorConcurrentRunner",
    "AtomicConcurrentRunner",
    "ForkJoinRunner",
    "StreamConcurrentRunner"
    ]

dados = dados[dados["Class"].isin(classes)]

dados["Benchmark"] = dados.Benchmark.str.split("Runner.").str[1]
dados["Threads"] = dados["Param: n_threads"].astype(int)
dados.dropna(subset = ['Score'], inplace=True)
dados.drop(["Param: dataset",
            "Param: stringManipulation",
            "Param: n_threads",
            "Param: buffer_size",
            "Samples", "Mode",
            "Score Error (99,9%)","Unit"],
            axis=1, inplace=True)

serial = dados[dados.Class == "NaiveSerialRunner"]
serial2 = serial.copy()
serial2["Threads"] = 2
serial4 = serial.copy()
serial4["Threads"] = 4
serial6 = serial.copy()
serial6["Threads"] = 6
serial8 = serial.copy()
serial8["Threads"] = 8
serial12 = serial.copy()
serial12["Threads"] = 12

data = pd.concat([dados, serial2, serial4, serial6, serial8, serial12])
data.reset_index(inplace=True, drop=True)

alvos=set(["compute_df", "compute_tfidf"])
plt.figure(figsize=(10,5))
g = sns.relplot(x="Threads", y="Score", col="Benchmark",
                hue="Class", style="GC", kind="line", markers=True,
                ci="sd", palette="dark", alpha=.6,
                data=data[data["Benchmark"].isin(alvos)])

g.savefig("imgs/Throughput per Thread.png")



alvos=set(["compute_df:·gc.alloc.rate", "compute_tfidf:·gc.alloc.rate"])
plt.figure(figsize=(10,5))
g = sns.relplot(x="Threads", y="Score", col="Benchmark",
                hue="Class", style="GC", kind="line",
                ci="sd", palette="dark", alpha=.6,
                data=data[data["Benchmark"].isin(alvos)])
g.savefig("imgs/GC allocation rate per Thread.png")

dataC = data[data.Class != "NaiveSerialRunner"]
alvos=set(["compute_df:·gc.alloc.rate", "compute_tfidf:·gc.alloc.rate"])
plt.figure(figsize=(10,5))
g = sns.relplot(x="Threads", y="Score", col="Benchmark",
                hue="Class", style="GC", kind="line",
                ci="sd", palette="dark", alpha=.6,
                data=dataC[dataC["Benchmark"].isin(alvos)])
g.savefig("imgs/Concurrent GC allocation rate per Thread.png")

alvos=set(["compute_df:·gc.time", "compute_tfidf:·gc.time"])
plt.figure(figsize=(10,5))
g = sns.relplot(x="Threads", y="Score", col="Benchmark",
                hue="Class", style="GC", kind="line",
                ci="sd", palette="dark", alpha=.6,
                data=data[data["Benchmark"].isin(alvos)])
g.savefig("imgs/GC time spent per Thread.png")

alvos=set(["compute_df:·gc.count", "compute_tfidf:·gc.count"])
plt.figure(figsize=(10,5))
g = sns.relplot(x="Threads", y="Score", col="Benchmark",
                hue="Class", style="GC", kind="line",
                ci="sd", palette="dark", alpha=.6,
                data=data[data["Benchmark"].isin(alvos)])
g.savefig("imgs/GC counts per Thread.png")