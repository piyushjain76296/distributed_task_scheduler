DROP TABLE IF EXISTS dead_letter_tasks;
DROP INDEX IF EXISTS idx_task_executions_retry;
ALTER TABLE task_executions DROP COLUMN max_attempts;
ALTER TABLE task_executions DROP COLUMN next_retry_at;
