# TODO: Update DB with output directory, num pages, and completion timestamp for tasks and jobs

## Steps to Complete

1. **Update `mark_task_completed` in `utils/tasks_repository.py`**:
   - ✅ Modified the function to accept `output_directory` and `num_pages` parameters.
   - ✅ Updated the SQL query to set `outputFilePath` to `output_directory`.

2. **Update `main.py`**:
   - ✅ Modified the call to `mark_task_completed` to pass `result['base_path']` as `output_directory` and `result['pages_extracted']` as `num_pages`.
   - ✅ Added logic after processing all tasks to check if all tasks have status 'completed'. If yes, mark job completed; otherwise, mark job failed.

3. **Test the changes**:
   - Run the pipeline to ensure DB updates correctly.
   - Verify that outputFilePath is updated for tasks, and job status is set based on task statuses.
