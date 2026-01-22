from datetime import datetime
from typing import List, Dict, Optional

from psycopg2 import sql
from utils.connect_db import db_connect

# ---------------------------------
# CONSTANTS
# ---------------------------------
DUMMY_TASK_SUMMARY = (
    "Hey! This is a dummy summary for your file. "
    "We’re working hard to make it real."
)

DUMMY_JOB_SUMMARY = (
    "Hey! This is a dummy summary for your job. "
    "We’re working hard to make it real."
)


# ---------------------------------
# JOB QUERIES
# ---------------------------------
def get_job_by_id(job_id: str) -> Optional[Dict]:
    query = """
        SELECT *
        FROM "Job"
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        return cur.fetchone()


def mark_job_in_progress(job_id: str):
    query = """
        UPDATE "Job"
        SET "status" = 'in_progress',
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        conn.commit()


def mark_job_completed(job_id: str):
    query = """
        UPDATE "Job"
        SET "status" = 'completed',
            "outputSummary" = %s,
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (DUMMY_JOB_SUMMARY, job_id))
        conn.commit()


def mark_job_failed(job_id: str, reason: str = None):
    query = """
        UPDATE "Job"
        SET "status" = 'failed',
            "outputSummary" = %s,
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    summary = reason or "Job failed during processing."

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (summary, job_id))
        conn.commit()


# ---------------------------------
# TASK QUERIES
# ---------------------------------
def get_tasks_for_job(job_id: str) -> List[Dict]:
    query = """
        SELECT *
        FROM "Task"
        WHERE "jobId" = %s
        ORDER BY "createdAt" ASC
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        return cur.fetchall()


def mark_task_in_progress(task_id: str, pid: int):
    query = """
        UPDATE "Task"
        SET "status" = 'in_progress',
            "pid" = %s,
            "startTs" = NOW(),
            "updatedAt" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (pid, task_id))
        conn.commit()


def mark_task_completed(task_id: str, output_directory: str, num_pages: int):
    query = """
        UPDATE "Task"
        SET "status" = 'completed',
            "outputSummary" = %s,
            "outputFilePath" = %s,
            "endTs" = NOW(),
            "updatedAt" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (DUMMY_TASK_SUMMARY, output_directory, task_id))
        conn.commit()


def mark_task_failed(task_id: str, reason: str = None):
    query = """
        UPDATE "Task"
        SET "status" = 'failed',
            "outputSummary" = %s,
            "endTs" = NOW(),
            "updatedAt" = NOW()
        WHERE "id" = %s
    """

    summary = reason or "Task failed during processing."

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (summary, task_id))
        conn.commit()
