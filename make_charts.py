import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

os.makedirs("figures", exist_ok=True)
df = pd.read_csv("benchmark_results.csv")
avg = df.groupby(["cores", "mode"])["seconds"].mean().unstack().sort_index()
print(avg.round(2))
print("Federated speedup vs 1 core:")
print((avg.loc[1, "federated"] / avg["federated"]).round(2))

# Figure 21: grouped bars
ax = avg[["federated", "centralized"]].plot(kind="bar", figsize=(7, 4.5), rot=0,
                                            color=["#2a6fdb", "#e07b39"])
ax.set_xlabel("CPU cores (local[N])")
ax.set_ylabel("Average runtime (seconds)")
ax.set_title("Federated vs Centralized Processing Time")
for c in ax.containers:
    ax.bar_label(c, fmt="%.1f", fontsize=8)
plt.tight_layout()
plt.savefig("figures/Fig 21 Performance Benchmark.png", dpi=600)
plt.close()

# Figure 22: scalability lines
fig, ax = plt.subplots(figsize=(7, 4.5))
ax.plot(avg.index, avg["federated"], "o-", label="Federated", color="#2a6fdb")
ax.plot(avg.index, avg["centralized"], "s--", label="Centralized", color="#e07b39")
ax.set_xticks(list(avg.index))
ax.set_xlabel("CPU cores (local[N])")
ax.set_ylabel("Average runtime (seconds)")
ax.set_title("Scalability: Runtime vs Number of Cores")
ax.grid(alpha=0.3)
ax.legend()
plt.tight_layout()
plt.savefig("figures/Fig 22 Scalability Evaluation.png", dpi=600)
plt.close()
