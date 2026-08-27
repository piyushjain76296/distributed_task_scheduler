-- name: CreateTenant :one
INSERT INTO tenants (name)
VALUES ($1)
RETURNING *;

-- name: CreateWorkflow :one
INSERT INTO workflows (tenant_id, name, definition, version)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetWorkflow :one
SELECT * FROM workflows
WHERE id = $1;

-- name: CreateWorkflowExecution :one
INSERT INTO workflow_executions (tenant_id, workflow_id, idempotency_key, status)
VALUES ($1, $2, $3, 'PENDING')
RETURNING *;

-- name: UpdateWorkflowExecutionStatus :one
UPDATE workflow_executions
SET status = $2, started_at = $3, completed_at = $4, updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING *;

-- name: CreateWorkflowTask :one
INSERT INTO workflow_tasks (workflow_id, task_name, task_type, dependencies, priority)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetWorkflowTasks :many
SELECT * FROM workflow_tasks
WHERE workflow_id = $1;
