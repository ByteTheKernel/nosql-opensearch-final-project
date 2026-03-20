#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from typing import Any, Dict, Optional

import requests


DEFAULT_OPENSEARCH_URL = "http://localhost:9200"


def request_json(method: str, url: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        response = requests.request(
            method=method,
            url=url,
            json=body,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        print(f"[ERROR] Request failed: {exc}", file=sys.stderr)
        sys.exit(1)


def pretty_print(data: Dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def get_student(base_url: str, student_id: str) -> None:
    url = f"{base_url}/students/_doc/{student_id}"
    result = request_json("GET", url)
    pretty_print(result)


def search_students_by_faculty(base_url: str, faculty: str, size: int) -> None:
    url = f"{base_url}/students/_search"
    body = {
        "size": size,
        "query": {
            "term": {
                "faculty": faculty
            }
        },
        "_source": [
            "student_id",
            "full_name",
            "faculty",
            "program",
            "year",
            "gpa",
            "city"
        ]
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def search_students_by_program(base_url: str, program: str, size: int) -> None:
    url = f"{base_url}/students/_search"
    body = {
        "size": size,
        "query": {
            "term": {
                "program": program
            }
        },
        "_source": [
            "student_id",
            "full_name",
            "faculty",
            "program",
            "year",
            "gpa",
            "city"
        ]
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def search_students_by_course(base_url: str, course_id: str, size: int) -> None:
    url = f"{base_url}/students/_search"
    body = {
        "size": size,
        "query": {
            "nested": {
                "path": "courses",
                "query": {
                    "term": {
                        "courses.course_id": course_id
                    }
                }
            }
        },
        "_source": [
            "student_id",
            "full_name",
            "faculty",
            "program",
            "year",
            "gpa",
            "courses"
        ]
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def top_faculties(base_url: str, size: int) -> None:
    url = f"{base_url}/students/_search"
    body = {
        "size": 0,
        "aggs": {
            "by_faculty": {
                "terms": {
                    "field": "faculty",
                    "size": size
                }
            }
        }
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def average_gpa(base_url: str) -> None:
    url = f"{base_url}/students/_search"
    body = {
        "size": 0,
        "aggs": {
            "avg_gpa": {
                "avg": {
                    "field": "gpa"
                }
            }
        }
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def activity_stats(base_url: str, size: int) -> None:
    url = f"{base_url}/student_activity/_search"
    body = {
        "size": 0,
        "aggs": {
            "by_event_type": {
                "terms": {
                    "field": "event_type",
                    "size": size
                }
            },
            "by_source": {
                "terms": {
                    "field": "source",
                    "size": size
                }
            },
            "avg_duration_ms": {
                "avg": {
                    "field": "details.duration_ms"
                }
            }
        }
    }
    result = request_json("GET", url, body)
    pretty_print(result)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simple Python CLI for the OpenSearch NoSQL final project."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_OPENSEARCH_URL,
        help=f"OpenSearch base URL (default: {DEFAULT_OPENSEARCH_URL})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    get_student_parser = subparsers.add_parser("get-student", help="Get student by ID")
    get_student_parser.add_argument("--student-id", required=True)

    faculty_parser = subparsers.add_parser("search-faculty", help="Search students by faculty")
    faculty_parser.add_argument("--faculty", required=True)
    faculty_parser.add_argument("--size", type=int, default=5)

    program_parser = subparsers.add_parser("search-program", help="Search students by program")
    program_parser.add_argument("--program", required=True)
    program_parser.add_argument("--size", type=int, default=5)

    course_parser = subparsers.add_parser("search-course", help="Search students by course")
    course_parser.add_argument("--course-id", required=True)
    course_parser.add_argument("--size", type=int, default=5)

    top_faculties_parser = subparsers.add_parser("top-faculties", help="Show top faculties")
    top_faculties_parser.add_argument("--size", type=int, default=10)

    subparsers.add_parser("average-gpa", help="Show average GPA")

    activity_parser = subparsers.add_parser("activity-stats", help="Show activity statistics")
    activity_parser.add_argument("--size", type=int, default=10)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = args.url.rstrip("/")

    if args.command == "get-student":
        get_student(base_url, args.student_id)
    elif args.command == "search-faculty":
        search_students_by_faculty(base_url, args.faculty, args.size)
    elif args.command == "search-program":
        search_students_by_program(base_url, args.program, args.size)
    elif args.command == "search-course":
        search_students_by_course(base_url, args.course_id, args.size)
    elif args.command == "top-faculties":
        top_faculties(base_url, args.size)
    elif args.command == "average-gpa":
        average_gpa(base_url)
    elif args.command == "activity-stats":
        activity_stats(base_url, args.size)
    else:
        print(f"[ERROR] Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()