# godisruptor

An implementation of the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) in Go.

The library follows the architecture of LMAX Disruptor 4: sequences, a preallocated ring buffer, single and multi producer sequencers, sequence barriers, pluggable wait strategies (busy spin, yielding, sleeping, blocking), batch event processors and a DSL for wiring a graph of consumers.

## Goals

- No dependencies beyond the Go standard library.
- No allocation on the hot path, so the garbage collector never runs because of the Disruptor. Generic over the event type, with a small, fixed number of indirect calls per event.
- Every component stress tested under the race detector, and with `GOMAXPROCS` from 1 up to the number of cores.
- Honest evaluation. Throughput with repeated interleaved runs and benchstat, latency with an open loop load generator and percentiles, CPU cost reported next to latency, and comparisons against a buffered channel, a mutex based queue, smarty/go-disruptor and the Workiva lock free ring buffer.
- An order matching engine built on the library, measured end to end against the same engine built on channels.

## Requirements

- Go 1.24+

## Build

```bash
go vet ./...
go test ./...
go test -race ./...
```
