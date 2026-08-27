# Distributed Fault-Tolerant Task Scheduler & Workflow Orchestrator

## Project Overview
This project is a production-grade Distributed Fault-Tolerant Task Scheduler and Workflow Orchestrator written in Go. Inspired by the architectural ideas behind Temporal and Apache Airflow, the system allows users to define directed acyclic graph (DAG) workflows composed of multiple tasks and ensures their reliable execution across a fleet of distributed workers. 

It is designed to survive scheduler crashes, worker failures, network partitions, and database restarts.

## Key Features
- **Workflow Engine**: Supports complex DAG-based workflow executions.
- **Raft Consensus**: Utilizes HashiCorp Raft for electing a single leader scheduler to prevent split-brain scheduling.
- **Lease-based Task Assignment**: Ensures tasks are atomicly claimed and safely reassigned if a worker dies mid-execution.
- **gRPC Communication**: High-performance bidirectional streaming between the scheduler and the worker nodes.
- **At-Least-Once Execution**: Prioritizes state safety and idempotency.
- **Authoritative State**: Uses PostgreSQL for strict ACID guarantees and reliable queue state transitions.
- **Fault-Tolerant**: Includes dead-letter queues, worker heartbeat monitoring, and configurable retry policies with exponential backoff.

## Architecture
The system consists of the following high-level components:
1. **API Gateway**: REST API for users to define workflows and monitor executions.
2. **Scheduler Cluster**: A 3-node Raft consensus cluster that makes authoritative scheduling decisions, manages worker leases, and transitions state.
3. **Worker Pool**: Independent nodes that execute tasks and stream results back via gRPC.
4. **PostgreSQL**: The sole source of truth for tenants, workflows, queues, and audit logs.

## Development Status
This repository is currently under active development. Core infrastructure, database schemas (`sqlc`), and gRPC protobufs have been established.
