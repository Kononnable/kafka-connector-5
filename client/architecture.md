# Architecture

## Overview

Single background thread running an I/O event loop. All Kafka state lives on that thread. The public API sends commands to the event loop through a command channel. Data flows back to the user through separate per-producer/per-consumer channels (delivery oneshots for producer, records channel for consumer).

```
+--------------------------+     +-------------------------------+
|  User thread             |     |  Event loop thread            |
|                          |     |                               |
|  KafkaCluster            |cmd->|  loop {                       |
|  .producer(opts)         |     |    epoll_wait(timeout)        |
|  .consumer(opts)         |     |    drain_channel_commands()   |
|                          |     |    tick_state_machines()      |
|  Producer.send(rec)      |cmd->|    io_read_write()            |
|  Producer.flush()        |cmd->|  }                            |
|  Producer.close()        |cmd->|                               |
|                          |     |                               |
|  Consumer.subscribe()    |cmd->|                               |
|  Consumer.commit()       |cmd->|                               |
|  Consumer.close()        |cmd->|                               |
+--------------------------+     +-------------------------------+
```

## Key Components

### KafkaCluster

Main public api entrypoint. Holds shared config and spawns the event loop.

`KafkaOptions` contains cluster-level settings: `bootstrap_servers`, `metadata_max_age_ms`, `request_timeout_ms`, etc.

### Event Loop

A single `loop { epoll_wait -> process }` that owns everything:

| Owned state | Description |
|---|---|
| **Connection pool** | One `Connection` per broker. Each has: TCP socket, read/write buffers, connection state machine (CONNECTING -> API_VERSIONS -> [SASL] -> READY). |
| **Metadata cache** | `HashMap<BrokerId, BrokerInfo>` + `HashMap<(Topic, PartitionId), PartitionInfo>`. Refreshed via periodic MetadataRequest. |
| **Inflight requests** | Per-connection `HashMap<CorrelationId, InflightRequest>`. Tracks sent requests awaiting responses per broker. Used for timeout detection and response dispatch. |
| **Producers** | `Vec<ProducerState>`. Each has: `RecordAccumulator`, partitioner, per-send `oneshot` channels for delivery results. |
| **Consumers** | `Vec<ConsumerState>`. Each has: fetch state, offset state, group membership, `records_tx` mpsc channel to user. |
| **Deadlines** | Every loop iteration calls `tick(now)` on each state machine. The state machine compares `now` against its own internal deadlines (linger, delivery timeout, heartbeat interval, metadata age, request timeout) to decide if it should send a request, complete a delivery oneshot, or do nothing. |

Event loop tick:

```
fn tick():
    // 1. I/O — both directions
    for each ready socket:
        if readable:
            read bytes into connection buffer
            for each complete response (size prefix + body):
                deserialize body, find state machine by correlation_id
                state_machine.on_response(response)
        if writable:
            write pending bytes from connection buffer

    // 2. Channel commands from user thread
    for each queued command:
        match command:
            CreateProducer { opts, reply } -> register ProducerState
            CreateConsumer { opts, reply } -> register ConsumerState
            SendRecord { topic, key, value, headers, delivery_tx } -> partition -> accumulator
            Flush { reply } -> snapshot seq counter, reply when caught up
            Subscribe { topics, reply } -> attach reply for assignment result
            CommitOffsets { offsets, reply } -> enqueue OffsetCommitRequest
            Close -> graceful shutdown, then break loop

    // 3. State machine ticks — timing decisions
    //     Each state machine checks its own deadlines and enqueues requests
    //     or fires callbacks as needed.
    for each ProducerState { state.tick(now); }
    for each ConsumerState { state.tick(now); }
    for MetadataRefresh { state.tick(now); }
```

Each connection tracks:
- Socket fd
- Read buffer
- Write buffer
- Connection state: CONNECTING --> API_VERSIONS --> [SASL] --> READY
- Inflight requests: `HashMap<CorrelationId, InflightRequest>`
- ApiVersions cache (per connection, invalidated on disconnect)
- SASL state (if applicable)

### Producers

A `ProducerState` holds:
- **RecordAccumulator**  -  per-partition queues of record batches. Configurable `batch_size` and `linger_ms`.
- **Partitioner**  -  runs on the event loop thread. Uses cached metadata to pick a partition per record.
- **Sequence counter**  -  monotonically increasing 64-bit counter assigned to each `SendRecord` command. Used by `flush()` to know when all sends up to that point have completed.
- **Delivery channels**  -  each `SendRecord` carries a `oneshot::Sender<Result<RecordMetadata>>`. When a ProduceResponse arrives, `on_response()` completes the corresponding oneshot, delivering the result directly to the user thread without event loop involvement.

### Consumers

A `ConsumerState` holds:
- **Subscription state**  -  subscribed topics/partitions, group membership
- **Fetch state**  -  current offset per partition, whether a fetch is in flight
- **`records_tx`**  -  mpsc sender. When a FetchResponse arrives, `on_response()` parses the records and pushes them directly through this channel to the user. The event loop never touches user records.
- **Group coordinator**  -  resolved broker for group management
- **Heartbeat timer**  -  periodic heartbeat or ConsumerGroupHeartbeat (KIP-848)

The event loop keeps a fetch in flight for subscribed/assigned partitions when possible. When a FetchResponse arrives, records are pushed through `records_tx`. The next fetch is sent only if there is enough room in the receive buffer to respect the configured size limits.

## Wakeup Mechanism

The event loop uses a wakeup mechanism so it can receive commands from the user thread without polling. A separate wakeup fd (`eventfd` on Linux, or a pipe) is added to the epoll set alongside broker sockets. When the user thread sends a command through the channel, it also writes to the wakeup fd. This makes `epoll_wait` return immediately. The event loop then reads the wakeup fd (different code path from broker sockets), drains the command channel, and goes back to waiting.

## Startup Sequence (Producer Example)

```
User: client = KafkaCluster::new(options)
  -> Event loop starts, connects to bootstrap broker
  -> ApiVersionsRequest -> get supported versions
  -> MetadataRequest -> learn cluster topology
  -> Start periodic metadata refresh timer

User: producer = client.producer(opts)
  -> Event loop registers ProducerState with accumulator

User: producer.send(record)
  -> Channel: SendRecord { topic, key, value, headers, delivery_tx }
  -> Event loop: partition -> append to accumulator
  -> Next tick: linger expired -> build ProduceRequest -> send to leader broker
  -> Response arrives -> ProducerState.on_response() completes delivery_tx oneshot

User: producer.flush(timeout)
  -> Channel: Flush { seq, reply }
  -> Event loop: when all up to seq complete -> fire reply
  -> User thread unblocks
```

## State Machines

The event loop is a thin reactor. It handles I/O multiplexing, request/response correlation, and state machine lifecycle. All Kafka semantics live in the state machines:

```
+-------------------+  tick(now)        +---------------------+
|  Event Loop       |  on_response(resp) |  State Machine      |
|  (I/O reactor)    |------------------>|  (pure logic,       |
|                   |                    |   no I/O)           |
|  owned:           |                    |                     |
|  sockets, buffers |                    +---------------------+
+-------------------+                                           
```

| State Machine | Description |
|---|---|
| MetadataRefresh | Periodic MetadataRequest, cache updates |
| ProducerState | Batching, partition assignment, linger/delivery deadlines |
| ConsumerState | Fetch scheduling, offset tracking, rebalance protocol |

Each state machine is a plain struct with `tick(&mut self, now)` and `on_response(&mut self, response)`. No I/O, no threads. The event loop calls these methods as part of its tick. The state machine enqueues requests via the event loop's API. User-facing channels (`records_tx` for consumer, delivery oneshots for producer) live inside the state machines.

This separation keeps the event loop small and makes the Kafka logic testable in isolation without network.
