# Part A – Questions A.3 and A.4 (Kafka layer)

## A.3 – Partition count and key of each topic

| Topic | Partitions | Key | Justification |
|---|---|---|---|
| vehicle_events | 6 | vehicle_id | Kafka only guarantees ordering inside a partition. Same key = same partition = per-vehicle ordering preserved. 6 partitions allow 6 parallel reads (Spark reads one partition per task) without excessive overhead for 40 vehicles. |
| fleet_catalog | 3 | vehicle_id | The key is mandatory for compaction (Kafka keeps the latest value per key). Only 40 records: a few partitions are enough. |
| alerts | 3 | vehicle_id (to be confirmed) | Alerts of the same vehicle stay in order. 3 partitions allow up to 3 active notifiers; the volume is low. |
| region_kpis_1min | 3 | region (to be confirmed) | In update mode the same window is republished several times: the region key keeps these updates in order. |
| vehicle_events_dlq | 3 | vehicle_id if readable, otherwise none | Corrupted messages do not always have a vehicle_id; their order does not matter. |
| daily_reports | 1 | report date | One report per day: a single partition gives a total order, and no parallelism is needed. |

### Measurements on our broker

**Measurement 1 – simulator running for 15 s (321 messages):** each of the 40 vehicles is in a single partition.

| Partition | Vehicles | Messages |
|---|---|---|
| 0 | 7 | 50 |
| 1 | 4 | 26 |
| 2 | 8 | 51 |
| 3 | 8 | 74 |
| 4 | 5 | 52 |
| 5 | 8 | 68 |

The distribution is uneven (4 to 8 vehicles per partition, 26 to 74 messages) even without a "hot" vehicle: 40 keys hashed over 6 partitions never split perfectly. Consequence for Spark: some read tasks get more data than others.

**Measurement 2 – 30 messages of the same vehicle on a temporary 6-partition topic:**
- random key: messages scattered over all 6 partitions (3, 4, 6, 3, 9 and 5 messages);
- key = V0001: all 30 messages are in partition 2, the same partition as in measurement 1 (key hashing is deterministic).

### What happens if vehicle_events is keyed randomly?
Events of the same vehicle are spread over several partitions (measurement 2). Since each partition is read independently, the arrival order per vehicle is no longer guaranteed for a consumer that tracks the state of a vehicle. Nuance: Spark works in event time and reshuffles data for its aggregations anyway, so it is barely affected; per-vehicle ordering mostly matters for a plain consumer. Drawback of keying by vehicle: with --hot-vehicle, one vehicle sends 40% of the traffic to a single partition (skew, studied in part C).

### What happens with a single partition?
There is no parallelism: at most one consumer per group is active, a second notifier would stay idle, Spark would read with a single task, and throughput is limited to one partition. Adding partitions later also changes the key-to-partition mapping.

### Limit of our setup
Replication factor 1 and a single broker: only one copy of each partition, so no tolerance to a broker failure. In production we would use 3 copies.

## A.4 – Delivery semantics per hop

| Hop | Guarantee | Justification |
|---|---|---|
| Producer → broker | At least once, no retry duplicates | Idempotent producer: the broker assigns an identifier (PID) and numbers messages per partition, so a resend after a timeout is ignored. acks=all waits for all in-sync replicas (only one here). Limit: after a restart the producer gets a new PID, so there is no deduplication across two runs (hence the run id in event_id). |
| Broker → Spark | At least once | Spark records the offsets it reads in its checkpoint; after a crash it replays the last unfinished batch. |
| Spark → alerts | At least once | If Spark crashes after publishing a batch but before committing it, the batch is replayed: duplicate alerts are possible (see B.4). Mitigation: deterministic alert_id. |
| alerts → notifier | At least once | Manual commit after handling: a crash between the two makes the alert be handled again. Committing before handling would lose alerts. Handling must therefore be idempotent. |

## To check against the final code
- actual keys of alerts and region_kpis_1min (chosen in the Spark job);
- presence of an alert_id in the alerts;
- final commit strategy of the notifier.
