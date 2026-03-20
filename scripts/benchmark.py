#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


DEFAULT_SINGLE_URL = "http://localhost:9200"
DEFAULT_CLUSTER_URL = "http://localhost:9201"


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON at line {line_number} in {path}: {exc}"
                ) from exc
    return rows


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    sorted_values = sorted(values)
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)

    if f == c:
        return sorted_values[f]

    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return d0 + d1


def ensure_ok(response: requests.Response, context: str) -> None:
    if response.status_code not in (200, 201):
        raise RuntimeError(f"{context} failed: {response.status_code} {response.text}")


def check_opensearch(base_url: str) -> None:
    try:
        response = requests.get(base_url, timeout=15)
        ensure_ok(response, f"OpenSearch check at {base_url}")
    except requests.RequestException as exc:
        raise RuntimeError(f"OpenSearch is not reachable at {base_url}: {exc}") from exc


def delete_index(base_url: str, index_name: str) -> None:
    response = requests.delete(f"{base_url}/{index_name}", timeout=60)
    if response.status_code not in (200, 404):
        raise RuntimeError(
            f"Failed to delete index '{index_name}': {response.status_code} {response.text}"
        )


def create_index(base_url: str, index_name: str, body: Dict[str, Any]) -> None:
    response = requests.put(
        f"{base_url}/{index_name}",
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    ensure_ok(response, f"Create index '{index_name}'")


def refresh_index(base_url: str, index_name: str) -> None:
    response = requests.post(f"{base_url}/{index_name}/_refresh", timeout=60)
    ensure_ok(response, f"Refresh index '{index_name}'")


def get_count(base_url: str, index_name: str) -> int:
    response = requests.get(f"{base_url}/{index_name}/_count", timeout=60)
    ensure_ok(response, f"Count index '{index_name}'")
    return int(response.json()["count"])


def build_bulk_payload(index_name: str, docs: List[Dict[str, Any]], id_field: str) -> str:
    lines: List[str] = []
    for doc in docs:
        doc_id = doc.get(id_field)
        if not doc_id:
            raise ValueError(f"Document does not contain id field '{id_field}': {doc}")

        lines.append(
            json.dumps({"index": {"_index": index_name, "_id": doc_id}}, ensure_ascii=False)
        )
        lines.append(json.dumps(doc, ensure_ascii=False))

    return "\n".join(lines) + "\n"


def chunked(rows: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    return [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]


def bulk_insert_benchmark(
    base_url: str,
    index_name: str,
    docs: List[Dict[str, Any]],
    id_field: str,
    chunk_size: int,
) -> Dict[str, Any]:
    latencies_ms: List[float] = []
    total_docs = 0

    start_total = time.perf_counter()

    for batch in chunked(docs, chunk_size):
        payload = build_bulk_payload(index_name, batch, id_field)

        batch_start = time.perf_counter()
        response = requests.post(
            f"{base_url}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=payload.encode("utf-8"),
            timeout=120,
        )
        batch_end = time.perf_counter()

        ensure_ok(response, f"Bulk insert into '{index_name}'")
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"Bulk insert reported errors for '{index_name}'")

        latencies_ms.append((batch_end - batch_start) * 1000.0)
        total_docs += len(batch)

    refresh_index(base_url, index_name)

    end_total = time.perf_counter()
    total_time_sec = end_total - start_total
    docs_per_sec = total_docs / total_time_sec if total_time_sec > 0 else 0.0

    return {
        "operation_type": "insert",
        "index_name": index_name,
        "documents": total_docs,
        "chunk_size": chunk_size,
        "total_time_sec": round(total_time_sec, 6),
        "docs_per_sec": round(docs_per_sec, 6),
        "avg_latency_ms": round(statistics.mean(latencies_ms), 6) if latencies_ms else 0.0,
        "median_latency_ms": round(statistics.median(latencies_ms), 6) if latencies_ms else 0.0,
        "p95_latency_ms": round(percentile(latencies_ms, 95), 6),
        "p99_latency_ms": round(percentile(latencies_ms, 99), 6),
    }


def search_once(base_url: str, index_name: str, body: Dict[str, Any]) -> float:
    start = time.perf_counter()
    response = requests.get(
        f"{base_url}/{index_name}/_search",
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    end = time.perf_counter()

    ensure_ok(response, f"Search in '{index_name}'")
    result = response.json()
    if result.get("timed_out"):
        raise RuntimeError(f"Search timed out in '{index_name}'")

    return (end - start) * 1000.0


def search_benchmark(
    base_url: str,
    index_name: str,
    query_name: str,
    query_body: Dict[str, Any],
    repetitions: int,
) -> Dict[str, Any]:
    latencies_ms: List[float] = []

    start_total = time.perf_counter()
    for _ in range(repetitions):
        latencies_ms.append(search_once(base_url, index_name, query_body))
    end_total = time.perf_counter()

    total_time_sec = end_total - start_total
    ops_per_sec = repetitions / total_time_sec if total_time_sec > 0 else 0.0

    return {
        "operation_type": "search",
        "query_name": query_name,
        "index_name": index_name,
        "repetitions": repetitions,
        "total_time_sec": round(total_time_sec, 6),
        "ops_per_sec": round(ops_per_sec, 6),
        "avg_latency_ms": round(statistics.mean(latencies_ms), 6) if latencies_ms else 0.0,
        "median_latency_ms": round(statistics.median(latencies_ms), 6) if latencies_ms else 0.0,
        "p95_latency_ms": round(percentile(latencies_ms, 95), 6),
        "p99_latency_ms": round(percentile(latencies_ms, 99), 6),
    }


def save_results_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_result(row: Dict[str, Any]) -> None:
    print("-" * 80)
    for key, value in row.items():
        print(f"{key}: {value}")


def load_mapping(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_environment_benchmark(
    env_name: str,
    base_url: str,
    students_docs: List[Dict[str, Any]],
    activity_docs: List[Dict[str, Any]],
    students_mapping: Dict[str, Any],
    activity_mapping: Dict[str, Any],
    chunk_size: int,
    repetitions: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    students_index = "students_bench_py"
    activity_index = "student_activity_bench_py"

    print(f"\n=== Benchmark environment: {env_name} ({base_url}) ===")

    check_opensearch(base_url)

    delete_index(base_url, students_index)
    delete_index(base_url, activity_index)

    create_index(base_url, students_index, students_mapping)
    create_index(base_url, activity_index, activity_mapping)

    student_insert_result = bulk_insert_benchmark(
        base_url=base_url,
        index_name=students_index,
        docs=students_docs,
        id_field="student_id",
        chunk_size=chunk_size,
    )
    student_insert_result["environment"] = env_name
    results.append(student_insert_result)
    print_result(student_insert_result)

    activity_insert_result = bulk_insert_benchmark(
        base_url=base_url,
        index_name=activity_index,
        docs=activity_docs,
        id_field="event_id",
        chunk_size=chunk_size,
    )
    activity_insert_result["environment"] = env_name
    results.append(activity_insert_result)
    print_result(activity_insert_result)

    student_count = get_count(base_url, students_index)
    activity_count = get_count(base_url, activity_index)
    print(f"[INFO] {env_name}: inserted students = {student_count}, activity = {activity_count}")

    queries: List[Tuple[str, str, Dict[str, Any]]] = [
        (
            "term_faculty",
            students_index,
            {
                "track_total_hits": True,
                "query": {
                    "term": {
                        "faculty": "Computer Science"
                    }
                }
            },
        ),
        (
            "term_program",
            students_index,
            {
                "track_total_hits": True,
                "query": {
                    "term": {
                        "program": "Data Engineering"
                    }
                }
            },
        ),
        (
            "nested_course",
            students_index,
            {
                "track_total_hits": True,
                "query": {
                    "nested": {
                        "path": "courses",
                        "query": {
                            "term": {
                                "courses.course_id": "DB101"
                            }
                        }
                    }
                }
            },
        ),
        (
            "agg_faculty",
            students_index,
            {
                "size": 0,
                "aggs": {
                    "by_faculty": {
                        "terms": {
                            "field": "faculty"
                        }
                    }
                }
            },
        ),
        (
            "avg_gpa",
            students_index,
            {
                "size": 0,
                "aggs": {
                    "avg_gpa": {
                        "avg": {
                            "field": "gpa"
                        }
                    }
                }
            },
        ),
        (
            "activity_by_source",
            activity_index,
            {
                "size": 0,
                "aggs": {
                    "by_source": {
                        "terms": {
                            "field": "source"
                        }
                    }
                }
            },
        ),
    ]

    for query_name, index_name, query_body in queries:
        search_result = search_benchmark(
            base_url=base_url,
            index_name=index_name,
            query_name=query_name,
            query_body=query_body,
            repetitions=repetitions,
        )
        search_result["environment"] = env_name
        results.append(search_result)
        print_result(search_result)

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark one OpenSearch environment at a time."
    )
    parser.add_argument(
        "--environment",
        required=True,
        choices=["single-node", "cluster"],
        help="Which environment to benchmark",
    )
    parser.add_argument(
        "--single-url",
        default=DEFAULT_SINGLE_URL,
        help=f"Single-node OpenSearch URL (default: {DEFAULT_SINGLE_URL})",
    )
    parser.add_argument(
        "--cluster-url",
        default=DEFAULT_CLUSTER_URL,
        help=f"Cluster OpenSearch URL (default: {DEFAULT_CLUSTER_URL})",
    )
    parser.add_argument(
        "--students-file",
        default="data/students.jsonl",
        help="Path to students JSONL file",
    )
    parser.add_argument(
        "--activity-file",
        default="data/student_activity.jsonl",
        help="Path to student activity JSONL file",
    )
    parser.add_argument(
        "--single-students-mapping",
        default="mappings/students_mapping.json",
        help="Path to single-node students mapping",
    )
    parser.add_argument(
        "--single-activity-mapping",
        default="mappings/student_activity_mapping.json",
        help="Path to single-node activity mapping",
    )
    parser.add_argument(
        "--cluster-students-mapping",
        default="mappings/students_mapping_cluster.json",
        help="Path to cluster students mapping",
    )
    parser.add_argument(
        "--cluster-activity-mapping",
        default="mappings/student_activity_mapping_cluster.json",
        help="Path to cluster activity mapping",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Bulk insert chunk size (default: 1000)",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=100,
        help="Number of repetitions for each search query (default: 100)",
    )
    parser.add_argument(
        "--output-csv",
        required=True,
        help="Path to output CSV file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        students_docs = read_jsonl(args.students_file)
        activity_docs = read_jsonl(args.activity_file)

        if args.environment == "single-node":
            env_name = "single-node"
            base_url = args.single_url.rstrip("/")
            students_mapping = load_mapping(args.single_students_mapping)
            activity_mapping = load_mapping(args.single_activity_mapping)
        else:
            env_name = "cluster"
            base_url = args.cluster_url.rstrip("/")
            students_mapping = load_mapping(args.cluster_students_mapping)
            activity_mapping = load_mapping(args.cluster_activity_mapping)

        results = run_environment_benchmark(
            env_name=env_name,
            base_url=base_url,
            students_docs=students_docs,
            activity_docs=activity_docs,
            students_mapping=students_mapping,
            activity_mapping=activity_mapping,
            chunk_size=args.chunk_size,
            repetitions=args.repetitions,
        )

        output_path = Path(args.output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        save_results_csv(str(output_path), results)

        print("\n" + "=" * 80)
        print(f"[DONE] Benchmark completed successfully for: {env_name}")
        print(f"[DONE] Results saved to: {output_path}")
        print("=" * 80)

    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()