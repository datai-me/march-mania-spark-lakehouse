"""
Pipeline Runner

Usage:
  # list jobs:
  python jobs/run_pipeline.py

  # run range (numeric order):
  python jobs/run_pipeline.py 1 4

  # run explicit list (custom order):
  python jobs/run_pipeline.py --list 1,2,5,6,3,4
"""

import re
import sys
import subprocess
from pathlib import Path


JOBS_DIR = Path(__file__).parent


def discover_jobs():
    jobs = {}
    for file in JOBS_DIR.glob("*.py"):
        if file.name in {"run_pipeline.py"}:
            continue
        m = re.match(r"^(\d+)_.*\.py$", file.name)
        if m:
            jobs[int(m.group(1))] = file.name
    return dict(sorted(jobs.items()))


def run_job(job_file: str):
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


def parse_list_arg(s: str):
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def main():
    jobs = discover_jobs()
    if not jobs:
        print("No numbered jobs found.")
        sys.exit(1)

    args = sys.argv[1:]

    if not args:
        print("Available jobs:")
        for k, v in jobs.items():
            print(f"{k:02d} -> {v}")
        print("\nRun range: python jobs/run_pipeline.py 1 4")
        print("Run list : python jobs/run_pipeline.py --list 1,2,5,6,3,4")
        sys.exit(0)

    if args[0] == "--list":
        if len(args) < 2:
            print("Missing list. Example: --list 1,2,5,6,3,4")
            sys.exit(2)
        order = parse_list_arg(args[1])
        for n in order:
            if n not in jobs:
                print(f"⚠️ Job {n:02d} not found, skipping")
                continue
            run_job(jobs[n])
        print("\n🎯 Pipeline execution completed.")
        return

    # range mode
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
            print(f"⚠️ Job {number:02d} not found, skipping")

    print("\n🎯 Pipeline execution completed.")


if __name__ == "__main__":
    main()