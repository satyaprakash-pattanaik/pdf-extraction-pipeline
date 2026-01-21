import sys
import os
import time
import traceback

from utils.tasks_repository import (
    get_job_by_id,
    get_tasks_for_job,
    mark_job_in_progress,
    mark_job_completed,
    mark_job_failed,
    mark_task_in_progress,
    mark_task_completed,
    mark_task_failed,
)

# ---------------------------------
# ENTRY
# ---------------------------------
def main():
    if len(sys.argv) < 2:
        print("❌ Job ID not provided")
        sys.exit(1)

    job_id = sys.argv[1]
    pid = os.getpid()

    try:
        # ---------------------------------
        # VALIDATE JOB
        # ---------------------------------
        job = get_job_by_id(job_id)
        if not job:
            print(f"❌ Job not found: {job_id}")
            return

        print(f"🚀 Starting Job {job_id} (PID {pid})")

        # ---------------------------------
        # MARK JOB IN PROGRESS
        # ---------------------------------
        mark_job_in_progress(job_id)

        # ---------------------------------
        # FETCH TASKS
        # ---------------------------------
        tasks = get_tasks_for_job(job_id)
        if not tasks:
            print(f"⚠️ No tasks found for job {job_id}")
            mark_job_completed(job_id)
            return

        # ---------------------------------
        # PROCESS TASKS (DUMMY)
        # ---------------------------------
        for task in tasks:
            task_id = task["id"]
            file_name = task["fileName"]

            try:
                print(f"📄 Processing task {task_id} ({file_name})")

                mark_task_in_progress(task_id, pid)

                # -----------------------------
                # DUMMY PROCESSING
                # -----------------------------
                time.sleep(2)  # simulate work

                mark_task_completed(task_id)
                print(f"✅ Completed task {task_id}")

            except Exception as task_error:
                print(f"❌ Task failed: {task_id}")
                print(task_error)

                mark_task_failed(task_id, str(task_error))

        # ---------------------------------
        # MARK JOB COMPLETE
        # ---------------------------------
        mark_job_completed(job_id)
        print(f"🎉 Job {job_id} completed successfully")

    except Exception as e:
        print(f"🔥 Job {job_id} failed")
        print(traceback.format_exc())
        mark_job_failed(job_id, str(e))


# ---------------------------------
# BOOTSTRAP
# ---------------------------------
if __name__ == "__main__":
    main()
