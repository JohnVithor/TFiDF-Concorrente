# grafics.py

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

latencias = pd.read_csv("./latencias.csv", sep=";")
plt.figure(figsize=(10,5))
g = sns.relplot(x="Version", y="Latencia",
                hue="GC", kind="line", markers=True,
                ci="sd", palette="dark", alpha=.6,
                data=latencias, height=5, aspect=1.5)
plt.axhline(latencias['Latencia'].min(), color='red', label='Min', linewidth=0.5)
plt.tight_layout()
plt.savefig("imgs/Latencias.png")