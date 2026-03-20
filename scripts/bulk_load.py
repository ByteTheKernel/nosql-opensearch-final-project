#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from typing import Iterator, Dict, Any, List

import requests


DEFAULT_OPENSEARCH_URL = "http://localhost:9200"


def read_jsonl(path: str) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[ERROR] Invalid JSON in {path} at line {line_number}: {exc}",
                    file=sys.stderr,
                )
                sys.exit(1)


def build_bulk_payload(index_name: str, docs: List[Dict[str, Any]], id_field: str) -> str:
    lines = []
    for doc in docs:
        doc_id = doc.get(id_field)
        if not doc_id:
            print(
                f"[ERROR] Document does not contain id field '{id_field}': {doc}",
                file=sys.stderr,
            )
            sys.exit(1)

        action = {
            "index": {
                "_index": index_name,
                "_id": doc_id
            }
        }
        lines.append(json.dumps(action, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))

    return "\n".join(lines) + "\n"


def chunked(iterator: Iterator[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
    chunk = []
    for item in iterator:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def bulk_load(
    base_url: str,
    index_name: str,
    jsonl_path: str,
    id_field: str,
    chunk_size: int
) -> None:
    total_docs = 0

    for batch_number, docs in enumerate(chunked(read_jsonl(jsonl_path), chunk_size), start=1):
        payload = build_bulk_payload(index_name, docs, id_field)

        response = requests.post(
            f"{base_url}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=payload.encode("utf-8"),
            timeout=120,
        )

        if response.status_code not in (200, 201):
            print(
                f"[ERROR] Bulk request failed for batch {batch_number}: "
                f"{response.status_code} {response.text}",
                file=sys.stderr,
            )
            sys.exit(1)

        body = response.json()
        if body.get("errors"):
            print(
                f"[ERROR] OpenSearch reported errors in batch {batch_number}: {body}",
                file=sys.stderr,
            )
            sys.exit(1)

        total_docs += len(docs)
        print(f"[OK] Batch {batch_number}: loaded {len(docs)} docs into '{index_name}'")

    print(f"[DONE] Total loaded into '{index_name}': {total_docs}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk load JSONL data into OpenSearch.")
    parser.add_argument(
        "--url",
        default=DEFAULT_OPENSEARCH_URL,
        help=f"OpenSearch base URL (default: {DEFAULT_OPENSEARCH_URL})",
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Target OpenSearch index name",
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to JSONL file",
    )
    parser.add_argument(
        "--id-field",
        required=True,
        help="Field name to use as OpenSearch document _id",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Bulk batch size (default: 1000)",
    )
    args = parser.parse_args()

    bulk_load(
        base_url=args.url,
        index_name=args.index,
        jsonl_path=args.file,
        id_field=args.id_field,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    main()