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
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        conn.commit()

    return True


def mark_job_failed(job_id: str, reason: str = None):
    query = """
        UPDATE "Job"
        SET "status" = 'failed',
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
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
def create_tasks_for_job(job_id: str):
    """
    Create a task for each demand file in the job with dummy summary.

    Args:
        job_id: Job ID
    """
    # Get all demand files for the job that are not yet summarized
    demand_files_query = """
        SELECT df."id", df."fileName", df."filePath"
        FROM "DemandFile" df
        JOIN "Job" j ON j."demandNoteId" = df."demandNoteId"
        WHERE j."id" = %s AND df."summaryStatus" = 'not_summarized'
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(demand_files_query, (job_id,))
        demand_files = cur.fetchall()

        # Create a task for each demand file
        for df in demand_files:
            task_query = """
                INSERT INTO "Task" ("id", "jobId", "demandFileId", "fileName", "filePath", "outputSummary", "status", "createdAt", "updatedAt")
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, 'pending', NOW(), NOW())
            """
            cur.execute(task_query, (job_id, df['id'], df['fileName'], df['filePath'], DUMMY_TASK_SUMMARY))

        conn.commit()
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


    Args:
        task_id: Task ID
        output_directory: Path to output directory
        num_pages: Number of pages processed
        job_id: Job ID (to check if all tasks are done)
    """
    query = """
        UPDATE "Task"
        SET "status" = 'completed',
            "outputFilePath" = %s,
            "endTs" = NOW(),
            "editedSummaryTs" = NOW(),
            "updatedAt" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (output_directory, task_id))
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
            "endTs" = NOW(),
            "updatedAt" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (task_id,))
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


def get_task_by_demand_file_id(demand_file_id: str) -> Optional[Dict]:
    """
    Get task by demand file ID.

    Args:
        demand_file_id: DemandFile ID

    Returns:
        Task dictionary or None if not found
    """
    query = """
        SELECT *
        FROM "Task"
        WHERE "demandFileId" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (demand_file_id,))
        return cur.fetchone()


# ---------------------------------
# DEMAND FILE QUERIES
# ---------------------------------
def get_demand_files_for_job(job_id: str) -> List[Dict]:
    """
    Get demand files for a job that are not yet summarized.

    Args:
        job_id: Job ID

    Returns:
        List of demand file dictionaries
    """
    query = """
        SELECT df.*
        FROM "DemandFile" df
        JOIN "Job" j ON j."demandNoteId" = df."demandNoteId"
        WHERE j."id" = %s AND df."summaryStatus" = 'not_summarized'
        ORDER BY df."createdAt" ASC
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        return cur.fetchall()


def mark_demand_file_summarized(demand_file_id: str, output_directory: str):
    """
    Mark demand file as summarized and update file path.

    Args:
        demand_file_id: DemandFile ID
        output_directory: Path to output directory
    """
    query = """
        UPDATE "DemandFile"
        SET "summaryStatus" = 'summarized',
            "filePath" = %s
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (output_directory, demand_file_id))
        conn.commit()


def check_all_demand_files_summarized(job_id: str) -> bool:
    """
    Check if all demand files for a job are summarized.

    Args:
        job_id: Job ID

    Returns:
        True if all demand files are summarized, False otherwise
    """
    query = """
        SELECT
            COUNT(*) as total_demand_files,
            COUNT(CASE WHEN "summaryStatus" = 'summarized' THEN 1 END) as summarized_files
        FROM "DemandFile" df
        JOIN "Job" j ON j."demandNoteId" = df."demandNoteId"
        WHERE j."id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        result = cur.fetchone()

        if not result:
            return False

        total = result.get('total_demand_files', 0)
        summarized = result.get('summarized_files', 0)

        # No demand files exist
        if total == 0:
            return False

        # All demand files are summarized
        return summarized == total


def mark_job_completed(job_id: str):
    """
    Mark job as completed only if all demand files are summarized.

    Args:
        job_id: Job ID

    Returns:
        bool: True if job was marked completed, False if not all demand files are done
    """
    # Check if all demand files are summarized first
    if not check_all_demand_files_summarized(job_id):
        return False

    query = """
        UPDATE "Job"
        SET "status" = 'completed',
            "updateTs" = NOW()
        WHERE "id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        conn.commit()

    return True


def check_and_update_job_status(job_id: str):
    """
    Check all demand files and update job status accordingly.
    - If all demand files are summarized: mark job as completed
    - Otherwise: keep job in progress

    Args:
        job_id: Job ID
    """
    query = """
        SELECT
            COUNT(*) as total_demand_files,
            COUNT(CASE WHEN "summaryStatus" = 'summarized' THEN 1 END) as summarized_files
        FROM "DemandFile" df
        JOIN "Job" j ON j."demandNoteId" = df."demandNoteId"
        WHERE j."id" = %s
    """

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(query, (job_id,))
        result = cur.fetchone()

        if not result:
            return

        total = result.get('total_demand_files', 0)
        summarized = result.get('summarized_files', 0)

        # No demand files exist - nothing to do
        if total == 0:
            return

        # All demand files summarized - mark job as completed
        if summarized == total:
            mark_job_completed(job_id)
