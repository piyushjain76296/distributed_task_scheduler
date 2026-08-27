ALTER TABLE task_executions ADD COLUMN next_retry_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE task_executions ADD COLUMN max_attempts INT NOT NULL DEFAULT 3;
CREATE INDEX idx_task_executions_retry ON task_executions(next_retry_at);

CREATE TABLE dead_letter_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_execution_id UUID NOT NULL REFERENCES task_executions(id),
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
