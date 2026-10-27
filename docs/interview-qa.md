# Interview Q&A

Common interview questions for each module, with an outline of a good answer. The outlines are meant as talking points, not scripts. Wherever possible they refer to something observed in this repository.

## Contents

- [Module 00: Kafka fundamentals](#module-00-kafka-fundamentals)

---

## Module 00: Kafka fundamentals

### 1. Why put a message broker between services instead of calling them over HTTP?

- With direct HTTP calls, services are coupled in time. If the receiver is down or slow, the caller fails or waits too.
- A broker gives temporal decoupling: the producer writes and moves on, and the consumer reads when it is ready.
- It also levels load: traffic spikes are buffered in the broker instead of overwhelming the consumer.
- New consumers can subscribe without any change to the producer.
- The costs: eventual consistency, one more piece of infrastructure to run, and harder debugging, because a request no longer follows a single call stack.
- HTTP is still the right choice when the caller needs an immediate answer, such as a query or a validation.

### 2. What is the difference between a queue and a log?

- In a queue (for example RabbitMQ), a message is removed after it is acknowledged, and the broker tracks delivery. Consumers compete for messages, which fits task distribution.
- In a log (Kafka), messages are appended and kept for a retention period whether or not anyone has read them. Each consumer group tracks its own position (offset).
- A log allows replay and lets many independent groups read the same data.
- In my lab, two groups read the same seven messages independently, and resetting a group's offset replayed history.
- Kafka has also added queue-like consumption through share groups (KIP-932), covered in module 04.

### 3. How does Kafka guarantee ordering?

- Ordering is guaranteed only within a partition, not across a topic.
- To keep all events of one entity in order, use the entity ID as the message key. The partitioner computes `hash(key) % partitions`, so the same key always lands in the same partition.
- A partition is shared by many keys. A single-partition topic gives total ordering but no parallelism.
- Ordering can break in three ways: a different partitioner (the Java client uses murmur2, librdkafka uses CRC32), a change in the partition count, or producer retries without idempotence (module 03).
- In my lab, the Java CLI put `order-3` in P0, while the .NET client put it in P2 on a topic with the same partition count.

### 4. What are replicas and ISR, and what happens when a broker dies?

- Each partition has a replication factor. One replica is the leader and serves reads and writes. Followers pull (fetch) data from the leader.
- `Replicas` is the assignment plan. The ISR (in-sync replicas) are the replicas that are currently caught up with the leader.
- When a broker dies, the controller elects a new leader from the ISR for each partition that broker led, and removes the dead broker from every ISR.
- When the broker comes back, it fetches what it missed and rejoins the ISR. With automatic rebalancing, leadership returns to the preferred leader, which is the first replica in the list.
- A leader change only updates metadata. Moving data between brokers requires partition reassignment.
- In my lab, killing kafka-2 moved P1's leader to kafka-3 and shrank every ISR to two nodes, with no data loss.

### 5. What is KRaft, and why did Kafka remove ZooKeeper?

- KRaft stores cluster metadata in an internal Kafka log that is replicated among controller nodes with the Raft consensus protocol. ZooKeeper support was removed in Kafka 4.0.
- Benefits: one system to deploy and secure instead of two, faster controller failover, and support for more partitions.
- One controller is the active leader. Every metadata change needs a majority of voters. Three controllers tolerate one failure, and five tolerate two.
- Do not confuse the controller leader (one per cluster, for metadata) with partition leaders (one per partition, for data).
- Without a majority, no metadata change can be committed: no leader elections, no ISR changes, no topic creation. This prevents split brain. In CAP terms, Kafka chooses consistency over availability here.

### 6. How do consumer groups and offsets work?

- Within one group, each partition is read by exactly one consumer, so the group splits the work. Different groups read the same data independently.
- The committed offset is the next offset to read, not the last one processed. It is stored in the internal `__consumer_offsets` topic.
- Lag = log-end offset minus committed offset. Growing lag means the consumer cannot keep up.
- `auto.offset.reset` (and `--from-beginning` in the CLI) applies only when the group has no committed offset, or when the committed offset no longer exists.
- In my lab, a group that had already committed offsets received nothing with `--from-beginning`. The cause was the committed offset, not deduplication by key, because Kafka does not deduplicate.

### 7. What happens if you add partitions to an existing topic?

- `hash(key) % partitions` gives a different result, so many keys move to a different partition.
- Existing data stays where it is. Old events of a key are in one partition and new ones in another, so per-key ordering can break during the transition.
- The partition count can be increased but never decreased.
- Options: choose enough partitions up front, or create a new topic with the new count and migrate consumers in a controlled way.

### 8. What is the difference between an event and a command?

- An event states a fact that already happened, named in the past tense, such as `OrderPlaced`. The producer does not know or care who consumes it, and any number of services can react.
- A command asks a specific receiver to do something, such as `ProcessPayment`. It has one intended handler, and the handler may reject it.
- Both can travel over Kafka. The difference is ownership and coupling: events keep the producer independent of its consumers, while commands make the sender depend on the receiver.