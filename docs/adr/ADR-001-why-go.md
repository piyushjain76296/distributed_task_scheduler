# ADR-001: Why Go

## Problem
We need to select a primary programming language for building a distributed, fault-tolerant task scheduler and workflow orchestrator.

## Options
1. Java
2. Python
3. Go
4. Rust

## Decision
We chose **Go**.

## Reasoning
1. **Concurrency Model**: Go's goroutines and channels make it extremely efficient to model thousands of concurrent worker connections, leases, and timeouts without the overhead of heavy OS threads.
2. **Ecosystem**: Go has mature, production-proven libraries for exactly what we need (e.g., `hashicorp/raft`, `grpc-go`, `pgx`).
3. **Performance**: Go is compiled to machine code and offers excellent performance and low latency, which is critical for scheduling decisions.
4. **Simplicity**: The language encourages straightforward, readable code and clear error handling, making it easier to reason about failure modes.

## Trade-offs
- Less expressive type system compared to Rust or TypeScript.
- Error handling can be verbose.

## Consequences
- The entire backend logic (scheduler, workers, API) will be written in Go.
- We will rely heavily on standard library concurrency primitives (`context`, `sync`, `channels`).
