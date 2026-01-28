import sys
import os
import traceback
import time

from utils.tasks_repository import (
    get_job_by_id,
    get_demand_files_for_job,
    mark_job_in_progress,
    mark_job_failed,
    mark_demand_file_summarized,
    check_and_update_job_status,
    create_tasks_for_job,
    get_task_by_demand_file_id,
    mark_task_in_progress,
    mark_task_completed,
    mark_task_failed,
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
        # CREATE TASKS FOR EACH FILE
        # ---------------------------------
        create_tasks_for_job(job_id)

        # ---------------------------------
        # FETCH DEMAND FILES
        # ---------------------------------
        demand_files = get_demand_files_for_job(job_id)
        if not demand_files:
            print(f"⚠️ No demand files found for job {job_id}")
            # Mark job as completed if there are no demand files to process
            mark_job_failed(job_id, "No demand files found for this job")
            return

        print(f"📋 Found {len(demand_files)} demand file(s) to process")

        # ---------------------------------
        # PROCESS DEMAND FILES
        # ---------------------------------
        for idx, demand_file in enumerate(demand_files, 1):
            demand_file_id = demand_file["id"]
            file_name = demand_file["fileName"]

            # Get the task for this demand file
            task = get_task_by_demand_file_id(demand_file_id)
            if not task:
                print(f"⚠️ No task found for demand file {demand_file_id}, skipping")
                continue

            task_id = task["id"]

            try:
                print(f"\n{'='*60}")
                print(f"📄 Processing demand file {idx}/{len(demand_files)}: {demand_file_id}")
                print(f"   File: {file_name}")
                print(f"   Task: {task_id}")
                print(f"{'='*60}")

                # Mark task as in progress
                mark_task_in_progress(task_id, pid)

                # Add delay to allow UI to show status updates
                time.sleep(5)

                # -----------------------------
                # PDF EXTRACTION PIPELINE
                # -----------------------------
                result = process_pdf_extraction(demand_file_id)

                if result['success']:
                    print(f"  ✅ Extracted {result['pages_extracted']} pages")
                    print(f"  ✅ Created {len(result['files_created'])} files")
                    print(f"  ✅ Output directory: {result['base_path']}")

                    # Mark task as completed
                    mark_task_completed(
                        task_id=task_id,
                        output_directory=str(result['base_path']),
                        num_pages=result['pages_extracted'],
                        job_id=job_id
                    )

                    # Mark demand file as summarized and automatically check job status
                    mark_demand_file_summarized(
                        demand_file_id=demand_file_id,
                        output_directory=str(result['base_path'])
                    )
                    print(f"✅ Summarized demand file {demand_file_id}")
                else:
                    error_msg = result.get('error', 'PDF extraction failed')
                    print(f"  ❌ Extraction failed: {error_msg}")
                    # Mark task as failed
                    mark_task_failed(task_id, job_id, error_msg)
                    print(f"⚠️ Demand file {demand_file_id} not summarized due to error")

            except Exception as demand_file_error:
                print(f"\n❌ Demand file {demand_file_id} encountered an error:")
                print(f"   Error: {str(demand_file_error)}")
                print(f"   Traceback:")
                print(traceback.format_exc())
                # Mark task as failed
                mark_task_failed(task_id, job_id, str(demand_file_error))
                print(f"⚠️ Demand file {demand_file_id} not summarized due to error")

        # ---------------------------------
        # FINAL JOB STATUS CHECK
        # ---------------------------------
        # The job status is automatically updated after each demand file processing
        # But we do a final check to ensure consistency
        check_and_update_job_status(job_id)

        # Get final demand file statistics and job status
        final_demand_files = get_demand_files_for_job(job_id)
        summarized_count = sum(1 for df in final_demand_files if df.get('summaryStatus') == 'summarized')
        not_summarized_count = sum(1 for df in final_demand_files if df.get('summaryStatus') == 'not_summarized')

        final_job = get_job_by_id(job_id)
        if final_job:
            status = final_job.get('status')

            print(f"\n{'='*60}")
            print(f"📊 Job {job_id} Summary")
            print(f"{'='*60}")
            print(f"   Total demand files: {len(final_demand_files)}")
            print(f"   ✅ Summarized: {summarized_count}")
            if not_summarized_count > 0:
                print(f"   ⏳ Not Summarized: {not_summarized_count}")
            print(f"{'='*60}")

            if status == 'completed':
                print(f"🎉 Job Status: COMPLETED")
                print(f"   All {len(final_demand_files)} demand file(s) summarized successfully!")
            elif status == 'in_progress':
                print(f"🔄 Job Status: IN PROGRESS")
                print(f"   {not_summarized_count} demand file(s) still not summarized")
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