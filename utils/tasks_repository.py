from datetime import datetime
from typing import List, Dict, Optional

from psycopg2 import sql
from utils.connect_db import db_connect

# ---------------------------------
# CONSTANTS
# ---------------------------------
DUMMY_TASK_SUMMARY = (
    "Hey! This is a dummy summary for your file. "
    "We're working hard to make it real."
)

DUMMY_JOB_SUMMARY = (
    "Hey! This is a dummy summary for your job. "
    "We're working hard to make it real."
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


def check_all_tasks_completed(job_id: str) -> bool:
    """
    Check if all tasks for a job are completed.
    
    Args:
        job_id: Job ID
        
    Returns:
        True if all tasks are completed, False otherwise
    """
    query = """
        SELECT 
            COUNT(*) as total_tasks,
            COUNT(CASE WHEN "status" = 'completed' THEN 1 END) as completed_tasks,
            COUNT(CASE WHEN "status" = 'failed' THEN 1 END) as failed_tasks
        FROM "Task"
        WHERE "jobId" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        result = cur.fetchone()
        
        if not result:
            return False
        
        total = result.get('total_tasks', 0)
        completed = result.get('completed_tasks', 0)
        failed = result.get('failed_tasks', 0)
        
        # No tasks exist
        if total == 0:
            return False
        
        # All tasks are either completed or failed
        return (completed + failed) == total


def mark_job_completed(job_id: str):
    """
    Mark job as completed only if all tasks are completed.
    
    Args:
        job_id: Job ID
        
    Returns:
        bool: True if job was marked completed, False if not all tasks are done
    """
    # Check if all tasks are completed first
    if not check_all_tasks_completed(job_id):
        return False
    
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
    
    return True


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


def check_and_update_job_status(job_id: str):
    """
    Check all tasks and update job status accordingly.
    - If all tasks are completed: mark job as completed
    - If any task failed and all others are done: mark job as failed
    - Otherwise: keep job in progress
    
    Args:
        job_id: Job ID
    """
    query = """
        SELECT 
            COUNT(*) as total_tasks,
            COUNT(CASE WHEN "status" = 'completed' THEN 1 END) as completed_tasks,
            COUNT(CASE WHEN "status" = 'failed' THEN 1 END) as failed_tasks,
            COUNT(CASE WHEN "status" = 'in_progress' THEN 1 END) as in_progress_tasks,
            COUNT(CASE WHEN "status" = 'pending' THEN 1 END) as pending_tasks
        FROM "Task"
        WHERE "jobId" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        result = cur.fetchone()
        
        if not result:
            return
        
        total = result.get('total_tasks', 0)
        completed = result.get('completed_tasks', 0)
        failed = result.get('failed_tasks', 0)
        in_progress = result.get('in_progress_tasks', 0)
        pending = result.get('pending_tasks', 0)
        
        # No tasks exist - nothing to do
        if total == 0:
            return
        
        # All tasks completed - mark job as completed
        if completed == total:
            mark_job_completed(job_id)
        
        # All tasks done (completed or failed), at least one failed - mark job as failed
        elif (completed + failed) == total and failed > 0:
            mark_job_failed(job_id, f"{failed} out of {total} tasks failed.")
        
        # Otherwise, job remains in progress (has pending or in_progress tasks)


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


def mark_task_completed(task_id: str, output_directory: str, num_pages: int, job_id: str):
    """
    Mark task as completed and check if the job should be completed.
    
    Args:
        task_id: Task ID
        output_directory: Path to output directory
        num_pages: Number of pages processed
        job_id: Job ID (to check if all tasks are done)
    """
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
    
    # Check if all tasks are done and update job status accordingly
    check_and_update_job_status(job_id)


def mark_task_failed(task_id: str, job_id: str, reason: str = None):
    """
    Mark task as failed and check if the job should be marked as failed.
    
    Args:
        task_id: Task ID
        job_id: Job ID (to check if all tasks are done)
        reason: Failure reason
    """
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
    
    # Check if all tasks are done and update job status accordingly
    check_and_update_job_status(job_id)


def get_job_id_from_task(task_id: str) -> Optional[str]:
    """
    Get job ID from task ID.
    
    Args:
        task_id: Task ID
        
    Returns:
        Job ID or None if task not found
    """
    query = """
        SELECT "jobId"
        FROM "Task"
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (task_id,))
        result = cur.fetchone()
        return result.get('jobId') if result else None