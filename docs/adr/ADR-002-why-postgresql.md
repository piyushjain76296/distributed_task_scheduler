# ADR-002: Why PostgreSQL

## Problem
We need a robust, authoritative data store for persisting workflow definitions, active task queues, execution states, and audit logs.

## Options
1. Redis
2. MongoDB
3. Cassandra
4. PostgreSQL

## Decision
We chose **PostgreSQL**.

## Reasoning
1. **ACID Transactions**: PostgreSQL provides strong ACID guarantees, allowing us to safely claim tasks using `SELECT FOR UPDATE` and update state consistently in atomic transactions.
2. **Relational Integrity**: Workflows, tasks, executions, and workers naturally map to relational models with clear foreign key constraints.
3. **Maturity**: PostgreSQL is battle-tested in production environments.
4. **Tooling**: We can use `pgx` and `sqlc` for high-performance, type-safe database interactions in Go.

## Trade-offs
- PostgreSQL does not natively shard out-of-the-box as easily as Cassandra. We will need to scale vertically initially or use connection pooling efficiently.
- Heavy reliance on DB for state transitions could bottleneck if not indexed/architected carefully.

## Consequences
- All authoritative state transitions must go through a PostgreSQL transaction.
- We will not use Redis or memory as the source of truth for task completion.
