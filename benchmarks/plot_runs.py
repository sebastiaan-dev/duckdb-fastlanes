import pandas as pd
import matplotlib.pyplot as plt


def plot(benchmark):
    data = pd.read_csv(f"{benchmark}.csv")

    row_mean = data.mean(axis=1)
    row_std = data.std(axis=1)
    x = data.index

    overall_mean = data.values.flatten().mean()
    overall_std = data.values.flatten().std()

    plt.figure(figsize=(12, 8))
    plt.plot(x, row_mean, label="Row-wise Mean", color="blue")

    # Shade the region between (mean - std) and (mean + std) for row-wise values.
    plt.fill_between(
        x,
        row_mean - row_std,
        row_mean + row_std,
        color="blue",
        alpha=0.3,
        label="Row-wise ±1 Standard Deviation",
    )

    plt.axhline(y=overall_mean, color="red", linestyle="--", label="Overall Mean")

    plt.text(
        x.min(),
        overall_mean + overall_std,
        f"Overall Mean: {overall_mean:.2f}\nOverall Std: {overall_std:.2f}",
        color="red",
        fontsize=10,
        verticalalignment="bottom",
    )

    plt.xlabel("Index / Time Step")
    plt.ylabel("Value")
    plt.title(f"Row-wise Mean with Standard Deviation for {benchmark}")
    plt.legend()
    plt.grid(True)
    plt.show()


benchmarks = [
    "data_len_arr_buffer",
    "data_len_arr_memory",
    "data_max_memory",
    "data_tmp_memory",
    "data_tmp_buffer",
]

if __name__ == "__main__":
    for benchmark in benchmarks:
        plot(benchmark)
