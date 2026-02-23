"""
Pipeline Runner
Execute Spark jobs in numeric order.

Usage:
    python jobs/run_pipeline.py 1 4   # runs 01 -> 04
    python jobs/run_pipeline.py 3     # runs only 03
"""

import sys
import subprocess
from pathlib import Path


JOBS_DIR = Path(__file__).parent


def discover_jobs():
    """Return dict {number: filename} sorted by number."""
    jobs = {}
    for file in JOBS_DIR.glob("*.py"):
        if file.name.startswith("run_"):
            continue
        parts = file.name.split("_", 1)
        if parts[0].isdigit():
            jobs[int(parts[0])] = file.name
    return dict(sorted(jobs.items()))


def run_job(job_file: str):
    """Execute a Spark job via spark-submit."""
    print(f"\n=== Running {job_file} ===")
    result = subprocess.run(
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            str(JOBS_DIR / job_file),
        ],
        check=False,
    )
    if result.returncode != 0:
        print(f"❌ Job failed: {job_file}")
        sys.exit(result.returncode)
    print(f"✅ Completed: {job_file}")


def main():
    jobs = discover_jobs()

    if not jobs:
        print("No jobs found.")
        sys.exit(1)

    args = sys.argv[1:]

    if not args:
        print("Available jobs:")
        for k, v in jobs.items():
            print(f"{k:02d} -> {v}")
        sys.exit(0)

    if len(args) == 1:
        start = int(args[0])
        end = start
    else:
        start = int(args[0])
        end = int(args[1])

    for number in range(start, end + 1):
        if number in jobs:
            run_job(jobs[number])
        else:
            print(f"⚠️ Job {number:02d} not found")

    print("\n🎯 Pipeline execution completed.")


if __name__ == "__main__":
    main()