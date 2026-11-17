# godisruptor

A Go implementation of the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/). A producer hands events to a consumer through a preallocated ring buffer. This version is single producer, single consumer (SPSC).

<p align="center"><img src="assets/architecture.svg" width="600" alt="Architecture"></p>

## How it works

The producer writes each event into the next slot of the ring and publishes it, which moves the cursor. The consumer waits until the cursor reaches the sequence it wants, reads the event, and records how far it has read in its own sequence. Before the producer reuses a slot it checks that sequence, so it never overwrites an event that has not been read.

The two sides never call each other. They only read two counters, and each counter is written by one side alone. A sequence only grows, and `sequence % size` gives its slot, which is what makes the ring circular.

## Usage

```bash
go get github.com/nolannguyen1212/godisruptor
```

Create a sequencer, a ring buffer for your event type, and a sequence for the consumer. The wait strategy decides how the consumer waits, from `NewBusySpinWaitStrategy`, which keeps a core busy for the lowest latency, to `NewBlockingWaitStrategy`, which parks the goroutine.

```go
sequencer := godisruptor.NewSequencer(1024, godisruptor.NewYieldingWaitStrategy())
ring := godisruptor.NewRingBuffer[Order](sequencer)
consumer := godisruptor.NewSequence(godisruptor.InitialSequence)
sequencer.AddConsumerSequences(consumer)
barrier := sequencer.NewBarrier()
```

The producer fills a slot and publishes it in one call.

```go
ring.PublishEvent(func(order *Order) {
	order.Price = 101
})
```

The consumer asks the barrier for the next sequence, reads every event up to it, and then stores how far it has read.

```go
for next := int64(0); ; {
	available, err := barrier.WaitFor(next)
	if err != nil {
		return
	}

	for sequence := next; sequence <= available; sequence++ {
		fmt.Println(ring.Get(sequence).Price)
	}

	consumer.Store(available)
	next = available + 1
}
```

`barrier.Alert()` stops a consumer that is waiting, and `WaitFor` then returns an error. A sequencer must be used by one producer goroutine only.

## Build

Go 1.26 or newer, and nothing beyond the standard library.

```bash
go vet ./...
go test ./...
go test -race ./...
```
