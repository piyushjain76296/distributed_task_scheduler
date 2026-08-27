# ADR-004: Why gRPC

## Problem
We need a high-performance, robust communication mechanism between the distributed workers and the scheduler.

## Options
1. REST / HTTP JSON
2. Message Queue (RabbitMQ / Kafka)
3. gRPC (Protocol Buffers)

## Decision
We chose **gRPC**.

## Reasoning
1. **Performance**: Protocol buffers provide highly efficient binary serialization compared to JSON. HTTP/2 provides multiplexing.
2. **Streaming**: gRPC supports bidirectional streaming natively, which is excellent for long-lived worker connections (task streaming, heartbeats).
3. **Strong Typing**: `.proto` files provide a strict, language-agnostic contract between the scheduler and workers.
4. **Timeouts/Deadlines**: gRPC has built-in support for context deadlines and cancellations, critical for a fault-tolerant system.

## Trade-offs
- Requires code generation step (protoc).
- Debugging binary payload is harder than plain text JSON.
- Load balancing gRPC (HTTP/2) can be trickier than HTTP/1.1 (requires layer 7 load balancers or client-side LB).

## Consequences
- We will build the worker-scheduler communication entirely on gRPC.
- We need `protoc` compiler in our build chain.
