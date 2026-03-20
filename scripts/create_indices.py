#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from typing import Any, Dict

import requests


DEFAULT_OPENSEARCH_URL = "http://localhost:9200"
STUDENTS_INDEX = "students"
ACTIVITY_INDEX = "student_activity"


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def check_opensearch(url: str) -> None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ERROR] OpenSearch is not reachable at {url}: {exc}", file=sys.stderr)
        sys.exit(1)


def index_exists(base_url: str, index_name: str) -> bool:
    response = requests.head(f"{base_url}/{index_name}", timeout=10)
    return response.status_code == 200


def delete_index(base_url: str, index_name: str) -> None:
    response = requests.delete(f"{base_url}/{index_name}", timeout=30)
    if response.status_code not in (200, 404):
        print(
            f"[ERROR] Failed to delete index '{index_name}': "
            f"{response.status_code} {response.text}",
            file=sys.stderr,
        )
        sys.exit(1)

    if response.status_code == 200:
        print(f"[OK] Deleted index: {index_name}")
    else:
        print(f"[INFO] Index did not exist: {index_name}")


def create_index(base_url: str, index_name: str, body: Dict[str, Any]) -> None:
    response = requests.put(
        f"{base_url}/{index_name}",
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=30,
    )

    if response.status_code not in (200, 201):
        print(
            f"[ERROR] Failed to create index '{index_name}': "
            f"{response.status_code} {response.text}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[OK] Created index: {index_name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create OpenSearch indices for the final NoSQL project."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_OPENSEARCH_URL,
        help=f"OpenSearch base URL (default: {DEFAULT_OPENSEARCH_URL})",
    )
    parser.add_argument(
        "--students-mapping",
        default="mappings/students_mapping.json",
        help="Path to students mapping JSON",
    )
    parser.add_argument(
        "--activity-mapping",
        default="mappings/student_activity_mapping.json",
        help="Path to student_activity mapping JSON",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete indices first if they already exist",
    )
    args = parser.parse_args()

    check_opensearch(args.url)

    students_mapping = load_json(args.students_mapping)
    activity_mapping = load_json(args.activity_mapping)

    for index_name, mapping in (
        (STUDENTS_INDEX, students_mapping),
        (ACTIVITY_INDEX, activity_mapping),
    ):
        if index_exists(args.url, index_name):
            if args.recreate:
                delete_index(args.url, index_name)
            else:
                print(
                    f"[INFO] Index already exists: {index_name}. "
                    f"Use --recreate to delete and recreate it."
                )
                continue

        create_index(args.url, index_name, mapping)


if __name__ == "__main__":
    main()