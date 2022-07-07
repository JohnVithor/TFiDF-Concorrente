# grafics.py

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

thread = pd.read_csv("./JMeter/Thread.csv", sep=';')
thread["Class"] = "Thread"
exe = pd.read_csv("./JMeter/Executor.csv", sep=';')
exe["Class"] = "Executor"
ato = pd.read_csv("./JMeter/Atomic.csv", sep=';')
ato["Class"] = "Atomic"
fjp = pd.read_csv("./JMeter/ForkJoin.csv", sep=';')
fjp["Class"] = "ForkJoinPool"
stm = pd.read_csv("./JMeter/Stream.csv", sep=';')
stm["Class"] = "Stream"


data = pd.concat([thread,
                  exe,
                  ato,
                  fjp,
                  stm])

data.reset_index(inplace=True)
# plt.figure(figsize=(10,10))
g = sns.relplot(x="Threads", y="Throughput1",
                hue="Class", style="GC", kind="line", markers=True,
                ci="sd", palette="dark", alpha=.6,
                data=data, height=5, aspect=1.5)
# g.set(xscale="log")
g.savefig("imgs/Throughput per Thread.png")