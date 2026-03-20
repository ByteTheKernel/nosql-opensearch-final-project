#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_SINGLE_CSV = "benchmark/results_single.csv"
DEFAULT_CLUSTER_CSV = "benchmark/results_cluster.csv"
DEFAULT_OUTPUT_DIR = "benchmark/plots"
DEFAULT_COMBINED_CSV = "benchmark/results_combined.csv"


def load_csv(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path)


def ensure_output_dir(path: str) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_combined_csv(df: pd.DataFrame, path: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)


def plot_insert_docs_per_sec(df: pd.DataFrame, output_dir: Path) -> None:
    insert_df = df[df["operation_type"] == "insert"].copy()
    if insert_df.empty:
        return

    insert_df["label"] = insert_df["environment"] + " | " + insert_df["index_name"]

    plt.figure(figsize=(10, 6))
    plt.bar(insert_df["label"], insert_df["docs_per_sec"])
    plt.title("Bulk insert throughput (docs/sec)")
    plt.xlabel("Environment | Index")
    plt.ylabel("Docs per second")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(output_dir / "insert_docs_per_sec.png", dpi=150)
    plt.close()


def plot_search_metric(
    df: pd.DataFrame,
    metric_column: str,
    title: str,
    ylabel: str,
    filename: str,
) -> None:
    search_df = df[df["operation_type"] == "search"].copy()
    if search_df.empty:
        return

    pivot_df = search_df.pivot(
        index="query_name",
        columns="environment",
        values=metric_column,
    ).fillna(0)

    ax = pivot_df.plot(kind="bar", figsize=(11, 6))
    ax.set_title(title)
    ax.set_xlabel("Query")
    ax.set_ylabel(ylabel)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()


def plot_search_ops_per_sec(df: pd.DataFrame, output_dir: Path) -> None:
    plot_search_metric(
        df=df,
        metric_column="ops_per_sec",
        title="Search throughput (ops/sec)",
        ylabel="Operations per second",
        filename=str(output_dir / "search_ops_per_sec.png"),
    )


def plot_search_p95_latency(df: pd.DataFrame, output_dir: Path) -> None:
    plot_search_metric(
        df=df,
        metric_column="p95_latency_ms",
        title="Search p95 latency (ms)",
        ylabel="p95 latency, ms",
        filename=str(output_dir / "search_p95_latency_ms.png"),
    )


def plot_search_avg_latency(df: pd.DataFrame, output_dir: Path) -> None:
    plot_search_metric(
        df=df,
        metric_column="avg_latency_ms",
        title="Search average latency (ms)",
        ylabel="Average latency, ms",
        filename=str(output_dir / "search_avg_latency_ms.png"),
    )


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    numeric_candidates: List[str] = [
        "documents",
        "chunk_size",
        "total_time_sec",
        "docs_per_sec",
        "avg_latency_ms",
        "median_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "repetitions",
        "ops_per_sec",
    ]

    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build comparison plots from single-node and cluster benchmark CSV files."
    )
    parser.add_argument(
        "--single-csv",
        default=DEFAULT_SINGLE_CSV,
        help=f"Path to single-node benchmark CSV (default: {DEFAULT_SINGLE_CSV})",
    )
    parser.add_argument(
        "--cluster-csv",
        default=DEFAULT_CLUSTER_CSV,
        help=f"Path to cluster benchmark CSV (default: {DEFAULT_CLUSTER_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory for output plots (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--combined-csv",
        default=DEFAULT_COMBINED_CSV,
        help=f"Path to combined CSV file (default: {DEFAULT_COMBINED_CSV})",
    )
    args = parser.parse_args()

    single_df = load_csv(args.single_csv)
    cluster_df = load_csv(args.cluster_csv)

    combined_df = pd.concat([single_df, cluster_df], ignore_index=True)
    combined_df = normalize_numeric_columns(combined_df)

    output_dir = ensure_output_dir(args.output_dir)
    save_combined_csv(combined_df, args.combined_csv)

    plot_insert_docs_per_sec(combined_df, output_dir)
    plot_search_ops_per_sec(combined_df, output_dir)
    plot_search_p95_latency(combined_df, output_dir)
    plot_search_avg_latency(combined_df, output_dir)

    print("=" * 80)
    print("[DONE] Plots created successfully.")
    print(f"[DONE] Combined CSV: {args.combined_csv}")
    print(f"[DONE] Output directory: {output_dir}")
    print("[DONE] Generated files:")
    print(f"  - {output_dir / 'insert_docs_per_sec.png'}")
    print(f"  - {output_dir / 'search_ops_per_sec.png'}")
    print(f"  - {output_dir / 'search_p95_latency_ms.png'}")
    print(f"  - {output_dir / 'search_avg_latency_ms.png'}")
    print("=" * 80)


if __name__ == "__main__":
    main()