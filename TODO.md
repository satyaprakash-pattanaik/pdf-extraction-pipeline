# TODO: Update Code to Match New Database Schema

## Tasks to Complete
- [x] Update utils/tasks_repository.py:
  - [x] In mark_job_completed: Remove "outputSummary" field, keep only status and updateTs.
  - [x] In mark_job_failed: Remove "outputSummary" field, keep only status and updateTs.
  - [x] In mark_task_completed: Change "outputSummary" to "summaryStatus", keep "endTs" and "updatedAt".
  - [x] In mark_task_failed: Change "outputSummary" to "summaryStatus", keep "endTs" and "updatedAt".
- [x] Update main.py: Add import time and time.sleep(5) after mark_task_in_progress to allow UI to show status updates.
- [x] Test the changes to ensure compatibility with the new schema.

## Notes
- For DemandFile, update summaryStatus if needed, but no current functions exist in the code.
- Ensure all status fields are updated correctly as per schema defaults.
