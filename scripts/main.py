import pandas as pd

file_path = "tpch-sf1-fls.projected.csv"   # projected/filtered
file_path_no_filter = "tpch-sf1-fls.csv"   # baseline (no filter)

# --- Load ---
df_proj = pd.read_csv(file_path, sep="\t")
df_base = pd.read_csv(file_path_no_filter, sep="\t")

# --- Helper to average per query ---
def avg_by_query(df):
    out = (
        df.groupby("name", as_index=False)["timing"]
        .mean()
        .assign(query=lambda x: x["name"].str.extract(r"(q\d+)\.benchmark"))
    )
    # Keep only rows where query could be extracted (q01..)
    out = out.dropna(subset=["query"])
    # Keep only q01–q20 just in case (optional but tidy)
    out = out[out["query"].str.match(r"q(0?[1-9]|1[0-9]|20)$")]
    # Average again by query (in case multiple names map to the same query)
    out = out.groupby("query", as_index=False)["timing"].mean()
    return out

avg_proj = avg_by_query(df_proj).rename(columns={"timing": "timing_projected"})
avg_base = avg_by_query(df_base).rename(columns={"timing": "timing_baseline"})

# --- Join on query ---
merged = pd.merge(avg_base, avg_proj, on="query", how="outer")

# --- Differences ---
# Absolute difference in seconds: projected - baseline (negative means projected is faster)
merged["diff_seconds"] = merged["timing_projected"] - merged["timing_baseline"]

# Percent improvement vs baseline: positive % means projected is faster
merged["speedup_percent"] = (
        (merged["timing_baseline"] - merged["timing_projected"]) / merged["timing_baseline"] * 100
)

# --- Order by query number ---
merged["qnum"] = merged["query"].str.extract(r"q(\d+)").astype(int)
merged = merged.sort_values("qnum").drop(columns=["qnum"]).reset_index(drop=True)

# Optional: round for readability
merged["timing_baseline"] = merged["timing_baseline"].round(6)
merged["timing_projected"] = merged["timing_projected"].round(6)
merged["diff_seconds"] = merged["diff_seconds"].round(6)
merged["speedup_percent"] = merged["speedup_percent"].round(2)

# Show/select final columns
result = merged[["query", "timing_baseline", "timing_projected", "diff_seconds", "speedup_percent"]]
print(result)