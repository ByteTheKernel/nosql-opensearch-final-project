#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any


FIRST_NAMES = [
    "Ivan", "Petr", "Sergey", "Alexey", "Dmitry", "Nikita", "Andrey", "Mikhail",
    "Anna", "Maria", "Elena", "Olga", "Svetlana", "Daria", "Ekaterina", "Natalia"
]

LAST_NAMES = [
    "Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Vasiliev",
    "Volkov", "Fedorov", "Morozov", "Sokolov", "Lebedev", "Kozlov", "Novikov"
]

FACULTIES = [
    "Computer Science",
    "Data Engineering",
    "Applied Mathematics",
    "Economics",
    "Business Informatics",
    "Cybersecurity"
]

PROGRAMS = [
    "Data Engineering",
    "Software Engineering",
    "Applied Analytics",
    "Machine Learning",
    "Information Security",
    "Business Analytics"
]

CITIES = [
    "Moscow",
    "Saint Petersburg",
    "Kazan",
    "Novosibirsk",
    "Yekaterinburg",
    "Nizhny Novgorod",
    "Samara",
    "Perm"
]

TAGS = [
    "scholarship",
    "olympiad",
    "internship",
    "exchange",
    "research",
    "honors",
    "sports",
    "volunteer"
]

COURSES = [
    ("DB101", "Databases"),
    ("SE201", "Software Engineering"),
    ("ML301", "Machine Learning"),
    ("DE202", "Data Pipelines"),
    ("OS210", "Operating Systems"),
    ("CS105", "Algorithms"),
    ("AN220", "Analytics"),
    ("SEC310", "Information Security")
]

EVENT_TYPES = [
    "login",
    "logout",
    "submit_assignment",
    "view_course",
    "register_course",
    "exam_attempt",
    "library_visit",
    "dashboard_open"
]

SOURCES = [
    "python_cli",
    "web_portal",
    "mobile_app",
    "admin_panel"
]


def random_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_courses() -> List[Dict[str, Any]]:
    count = random.randint(2, 5)
    selected = random.sample(COURSES, count)
    result = []
    for course_id, course_name in selected:
        result.append({
            "course_id": course_id,
            "course_name": course_name,
            "grade": random.randint(6, 10)
        })
    return result


def random_tags() -> List[str]:
    count = random.randint(1, 3)
    return random.sample(TAGS, count)


def generate_students(count: int) -> List[Dict[str, Any]]:
    students = []
    now = datetime.now(timezone.utc)

    for i in range(1, count + 1):
        student_id = f"S{i:06d}"
        created_at = now - timedelta(days=random.randint(0, 365))

        students.append({
            "student_id": student_id,
            "full_name": random_name(),
            "faculty": random.choice(FACULTIES),
            "program": random.choice(PROGRAMS),
            "year": random.randint(1, 4),
            "age": random.randint(17, 26),
            "gpa": round(random.uniform(6.0, 10.0), 2),
            "city": random.choice(CITIES),
            "dormitory": random.choice([True, False]),
            "tags": random_tags(),
            "created_at": created_at.isoformat(),
            "courses": random_courses()
        })

    return students


def generate_activity(
    students: List[Dict[str, Any]],
    events_per_student: int
) -> List[Dict[str, Any]]:
    activities = []
    now = datetime.now(timezone.utc)
    event_counter = 1

    for student in students:
        for _ in range(events_per_student):
            timestamp = now - timedelta(
                days=random.randint(0, 180),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            course = random.choice(student["courses"])

            activities.append({
                "event_id": f"E{event_counter:08d}",
                "student_id": student["student_id"],
                "event_type": random.choice(EVENT_TYPES),
                "source": random.choice(SOURCES),
                "course_id": course["course_id"],
                "timestamp": timestamp.isoformat(),
                "details": {
                    "result": random.choice(["success", "success", "success", "failed"]),
                    "duration_ms": random.randint(50, 5000),
                    "ip": f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}"
                }
            })
            event_counter += 1

    return activities


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for the OpenSearch NoSQL project."
    )
    parser.add_argument(
        "--students",
        type=int,
        default=5000,
        help="Number of students to generate (default: 5000)"
    )
    parser.add_argument(
        "--events-per-student",
        type=int,
        default=5,
        help="Number of activity events per student (default: 5)"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory for JSONL files (default: data)"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    students = generate_students(args.students)
    activities = generate_activity(students, args.events_per_student)

    students_path = os.path.join(args.output_dir, "students.jsonl")
    activity_path = os.path.join(args.output_dir, "student_activity.jsonl")

    write_jsonl(students_path, students)
    write_jsonl(activity_path, activities)

    print(f"[OK] Generated students: {len(students)} -> {students_path}")
    print(f"[OK] Generated activity events: {len(activities)} -> {activity_path}")


if __name__ == "__main__":
    main()