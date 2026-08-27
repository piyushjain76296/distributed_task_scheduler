# ADR-003: Why Raft

## Problem
We need to elect a single leader among the scheduler nodes to make authoritative scheduling decisions (e.g. which worker gets which task, retries, etc.) to avoid split-brain issues.

## Options
1. Redis-based locking
2. PostgreSQL-based locking (e.g. advisory locks)
3. Zookeeper / etcd (external consensus)
4. Embedded Raft (HashiCorp Raft)

## Decision
We chose **Embedded Raft (HashiCorp Raft)**.

## Reasoning
1. **No External Dependencies**: We want to minimize infrastructure requirements. Integrating an external system like etcd or Zookeeper adds operational complexity.
2. **True Consensus**: Raft gives us strict quorum-based leader election, handling network partitions robustly.
3. **Maturity**: HashiCorp's Raft implementation is widely used (e.g. in Consul, Nomad) and production-ready in Go.
4. **State Machine**: We can replicate critical scheduler state/metadata through the Raft log in the future, providing an authoritative distributed state.

## Trade-offs
- Setting up HashiCorp Raft requires careful configuration of transports and snapshoting.
- We must handle split-brain in network partitions, which Raft natively solves if quorum is required.

## Consequences
- The scheduler binary will include Raft nodes. We will require at least 3 nodes to form a highly-available cluster.
