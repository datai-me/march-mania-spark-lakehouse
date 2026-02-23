"""
Pipeline runner that submits Spark jobs with required cluster + S3A + py-files settings.

Usage:
  python jobs/run_pipeline.py --list 1,2,5,6,3,4
  python jobs/run_pipeline.py 1 4
"""

import os
import re
import sys
import subprocess
from pathlib import Path


JOBS_DIR = Path(__file__).parent

SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
PYFILES = "/opt/project/src.zip"


def discover_jobs():
    jobs = {}
    for file in JOBS_DIR.glob("*.py"):
        if file.name == "run_pipeline.py":
            continue
        m = re.match(r"^(\d+)_.*\.py$", file.name)
        if m:
            jobs[int(m.group(1))] = file.name
    return dict(sorted(jobs.items()))


def spark_submit_args(app_path: str):
    return [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,

        # S3A/MinIO (stable)
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", f"spark.hadoop.fs.s3a.endpoint={os.environ.get('MINIO_ENDPOINT','http://minio:9000')}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={os.environ.get('MINIO_ACCESS_KEY','admin')}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={os.environ.get('MINIO_SECRET_KEY','admin123')}",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",

        # Python
        "--conf", "spark.pyspark.python=python",
        "--conf", "spark.pyspark.driver.python=python",
        "--conf", "spark.executorEnv.PYTHONPATH=/opt/project/src.zip:/opt/project",

        # Logging
        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/opt/project/conf/log4j2.properties",
        "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/opt/project/conf/log4j2.properties",

        # Ship project code to executors
        "--py-files", PYFILES,

        # App
        app_path,
    ]


def run_job(job_file: str):
    app = str(JOBS_DIR / job_file)
    print(f"\n=== Running {job_file} ===", flush=True)
    args = spark_submit_args(app)
    rc = subprocess.call(args)
    if rc != 0:
        print(f"❌ Job failed: {job_file}", flush=True)
        sys.exit(rc)
    print(f"✅ Completed: {job_file}", flush=True)


def parse_list_arg(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]


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
        print("\nRun list : python jobs/run_pipeline.py --list 1,2,5,6,3,4")
        print("Run range: python jobs/run_pipeline.py 1 4")
        sys.exit(0)

    if args[0] == "--list":
        order = parse_list_arg(args[1])
        for n in order:
            if n not in jobs:
                print(f"⚠️ Job {n:02d} not found, skipping")
                continue
            run_job(jobs[n])
        print("\n🎯 Pipeline completed.")
        return

    start = int(args[0])
    end = int(args[1]) if len(args) > 1 else start
    for n in range(start, end + 1):
        if n in jobs:
            run_job(jobs[n])
        else:
            print(f"⚠️ Job {n:02d} not found, skipping")
    print("\n🎯 Pipeline completed.")


if __name__ == "__main__":
    main()