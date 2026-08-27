-- name: RegisterWorker :one
INSERT INTO workers (id, hostname, version, max_concurrent_tasks, active_tasks, supported_task_types, last_heartbeat, status)
VALUES ($1, $2, $3, $4, 0, $5, CURRENT_TIMESTAMP, 'ONLINE')
ON CONFLICT (id) DO UPDATE
SET hostname = EXCLUDED.hostname,
    version = EXCLUDED.version,
    max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
    supported_task_types = EXCLUDED.supported_task_types,
    last_heartbeat = CURRENT_TIMESTAMP,
    status = 'ONLINE',
    updated_at = CURRENT_TIMESTAMP
RETURNING *;

-- name: UpdateWorkerHeartbeat :exec
UPDATE workers
SET active_tasks = $2,
    last_heartbeat = CURRENT_TIMESTAMP,
    status = 'ONLINE',
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1;

-- name: MarkStaleWorkersOffline :exec
UPDATE workers
SET status = 'OFFLINE',
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'ONLINE' AND last_heartbeat < $1;

-- name: GetWorker :one
SELECT * FROM workers
WHERE id = $1;
