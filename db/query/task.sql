-- name: CreateTaskExecution :one
INSERT INTO task_executions (workflow_execution_id, workflow_task_id, status, attempt)
VALUES ($1, $2, 'PENDING', 0)
RETURNING *;

-- name: GetPendingTasks :many
SELECT * FROM task_executions
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT $1::int;

-- name: ClaimTask :one
UPDATE task_executions
SET status = 'ASSIGNED',
    worker_id = $1,
    lease_expires_at = $2,
    attempt = attempt + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = (
    SELECT id FROM task_executions
    WHERE status = 'QUEUED' OR (status = 'ASSIGNED' AND lease_expires_at < CURRENT_TIMESTAMP)
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;

-- name: UpdateTaskExecutionStatus :one
UPDATE task_executions
SET status = $2,
    completed_at = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: RecordTaskAttempt :one
INSERT INTO task_attempts (task_execution_id, worker_id, attempt, status, error_message, completed_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;
