import sys
import os
import traceback

from utils.tasks_repository import (
    get_job_by_id,
    get_tasks_for_job,
    mark_job_in_progress,
    mark_job_failed,
    mark_task_in_progress,
    mark_task_completed,
    mark_task_failed,
    check_and_update_job_status,
)
from pipeline import process_pdf_extraction

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
            # Mark job as completed if there are no tasks to process
            mark_job_failed(job_id, "No tasks found for this job")
            return

        print(f"📋 Found {len(tasks)} task(s) to process")

        # ---------------------------------
        # PROCESS TASKS
        # ---------------------------------
        for idx, task in enumerate(tasks, 1):
            task_id = task["id"]
            file_name = task["fileName"]
            demand_file_id = task["demandFileId"]

            try:
                print(f"\n{'='*60}")
                print(f"📄 Processing task {idx}/{len(tasks)}: {task_id}")
                print(f"   File: {file_name}")
                print(f"{'='*60}")

                mark_task_in_progress(task_id, pid)

                # -----------------------------
                # PDF EXTRACTION PIPELINE
                # -----------------------------
                result = process_pdf_extraction(demand_file_id)

                if result['success']:
                    print(f"  ✅ Extracted {result['pages_extracted']} pages")
                    print(f"  ✅ Created {len(result['files_created'])} files")
                    print(f"  ✅ Output directory: {result['base_path']}")

                    # Mark task completed and automatically check job status
                    mark_task_completed(
                        task_id=task_id,
                        output_directory=str(result['base_path']),
                        num_pages=result['pages_extracted'],
                        job_id=job_id
                    )
                    print(f"✅ Completed task {task_id}")
                else:
                    error_msg = result.get('error', 'PDF extraction failed')
                    print(f"  ❌ Extraction failed: {error_msg}")
                    
                    # Mark task failed and automatically check job status
                    mark_task_failed(
                        task_id=task_id,
                        job_id=job_id,
                        reason=error_msg
                    )
                    print(f"❌ Failed task {task_id}")

            except Exception as task_error:
                print(f"\n❌ Task {task_id} encountered an error:")
                print(f"   Error: {str(task_error)}")
                print(f"   Traceback:")
                print(traceback.format_exc())

                # Mark task failed and automatically check job status
                mark_task_failed(
                    task_id=task_id,
                    job_id=job_id,
                    reason=str(task_error)
                )

        # ---------------------------------
        # FINAL JOB STATUS CHECK
        # ---------------------------------
        # The job status is automatically updated after each task completion/failure
        # But we do a final check to ensure consistency
        check_and_update_job_status(job_id)
        
        # Get final task statistics and job status
        final_tasks = get_tasks_for_job(job_id)
        completed_count = sum(1 for t in final_tasks if t.get('status') == 'completed')
        failed_count = sum(1 for t in final_tasks if t.get('status') == 'failed')
        in_progress_count = sum(1 for t in final_tasks if t.get('status') == 'in_progress')
        pending_count = sum(1 for t in final_tasks if t.get('status') == 'pending')
        
        final_job = get_job_by_id(job_id)
        if final_job:
            status = final_job.get('status')
            
            print(f"\n{'='*60}")
            print(f"📊 Job {job_id} Summary")
            print(f"{'='*60}")
            print(f"   Total tasks: {len(final_tasks)}")
            print(f"   ✅ Completed: {completed_count}")
            if failed_count > 0:
                print(f"   ❌ Failed: {failed_count}")
            if in_progress_count > 0:
                print(f"   🔄 In Progress: {in_progress_count}")
            if pending_count > 0:
                print(f"   ⏳ Pending: {pending_count}")
            print(f"{'='*60}")
            
            if status == 'completed':
                print(f"🎉 Job Status: COMPLETED")
                print(f"   All {len(final_tasks)} task(s) completed successfully!")
            elif status == 'failed':
                print(f"⚠️ Job Status: COMPLETED WITH FAILURES")
                print(f"   {completed_count} task(s) completed, {failed_count} task(s) failed")
            elif status == 'in_progress':
                print(f"🔄 Job Status: IN PROGRESS")
                remaining = in_progress_count + pending_count
                print(f"   {remaining} task(s) still processing or pending")
            else:
                print(f"ℹ️ Job Status: {status.upper()}")
            
            print(f"{'='*60}")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"🔥 Job {job_id} encountered a critical error")
        print(f"{'='*60}")
        print(f"Error: {str(e)}")
        print(f"\nFull traceback:")
        print(traceback.format_exc())
        
        mark_job_failed(job_id, f"Critical error: {str(e)}")
        sys.exit(1)


# ---------------------------------
# BOOTSTRAP
# ---------------------------------
if __name__ == "__main__":
    main()