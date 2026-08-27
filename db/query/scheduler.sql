-- name: ScheduleRetries :many
UPDATE task_executions
SET status = 'QUEUED',
    next_retry_at = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'RETRY_WAIT' AND next_retry_at <= CURRENT_TIMESTAMP
RETURNING *;

-- name: MoveToDeadLetter :one
INSERT INTO dead_letter_tasks (task_execution_id, last_error)
VALUES ($1, $2)
RETURNING *;

-- name: FailTaskExecution :one
UPDATE task_executions
SET status = 'FAILED',
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: MarkTaskForRetry :one
UPDATE task_executions
SET status = 'RETRY_WAIT',
    next_retry_at = $2,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;
