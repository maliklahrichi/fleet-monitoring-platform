# Part E - Failure experiments and performance study

## E.1 Killing the streaming job (kill -9)

**Hypothesis (written before the run).** Each query keeps in its checkpoint the Kafka offsets of
its last completed micro-batch. After a restart, Spark resumes from there. So:

- nothing is lost: messages produced while the job was down wait in Kafka and are read after the restart;
- Parquet rows are not duplicated: a replayed batch N rewrites its own folder `batch_id=N`
  (overwrite, dynamic mode), so the number of rows stays equal to the number of distinct `event_id`;
- point alerts can be duplicated: the Kafka write of a replayed batch is not idempotent, so the batch
  that was running when the job was killed may publish its alerts a second time (same `alert_id`);
- query 2 and query 3 restore their state from their own checkpoint directory.

**Observation.** The job was killed with `pkill -9` at 21:08:03 and started again by hand. The new
process started at 21:08:10, so there was no streaming job for about 7 seconds, while the simulator
kept sending 20 events per second. Counts taken while the simulator was still running:

| | Parquet rows | distinct event_id | alerts | distinct alert_id | DLQ messages | missing sequence numbers |
|---|---|---|---|---|---|---|
| Just before the kill | 10545 | 10545 | 330 | 330 | 117 | 330 of 11400 (2.9 %), longest hole 2 |
| After the restart | 12929 | 12929 | 414 | 414 | 155 | 434 of 14620 (3.0 %), longest hole 2 |

The number at the end of an `event_id` counts the events of one simulator run, so a hole in that
sequence is an event that never reached the Parquet files (`scripts/check_gaps.py`).

**Explanation.** Nothing was lost. A 7-second outage at 20 events per second is about 140 events; if
they had been lost we would see a hole of that length, but the longest hole is 2. The 3 % of missing
numbers is the same before and after, and it matches the simulator: 1 % corrupted messages (sent to
the DLQ) and 2 % late messages (3 to 6 minutes old, behind the 2-minute watermark, so dropped by
the deduplication). Nothing was duplicated in Parquet (rows equal distinct `event_id`): a replayed
batch rewrites its own `batch_id` folder. We did not observe duplicated alerts either (total equals
distinct), unlike what we expected. A duplicate needs the kill to land between the Kafka write of a
batch and the commit of its offsets in the checkpoint, a very short window, and one run does not
prove it cannot happen. This is why every alert has a deterministic `alert_id`.

## E.2 Stopping the alerts notifier for 10 minutes

**Hypothesis (written before the run).** While the notifier is stopped, nothing reads `alerts` and
the lag grows with the alert rate (about 3 % of the events, so roughly 0.6 alert per second over 3
partitions). The `kafka_lag_monitor` DAG should publish a `consumer_lag_high` alert for each partition
whose lag passes 10, at its next 5-minute run, and again at every run while the lag stays high. After
the restart the notifier resumes from the last committed offset (manual commit after each message), so
at most the message being handled when it stopped is delivered again, and it catches up in a few
seconds, since reading is much faster than the production rate.

## E.3 Stopping the Kafka broker for 1 minute

**Hypothesis (written before the run).**

- Producer (simulator): messages cannot be sent, so they pile up in the producer's local queue (20
  messages per second for 60 seconds is about 1200 messages, far below the default queue limit of
  100000). The client retries with no retry limit and its delivery timeout is 5 minutes, so after the
  broker comes back everything should be delivered, with zero failures and no loss; the idempotent
  producer prevents duplicates from the retries.
- Spark: the Kafka source retries its fetches. We expect the job to survive a one-minute outage and
  to catch up with a few large batches. It may also fail with a timeout if the outage is longer than
  the client timeouts, in which case it would have to be restarted and would resume from its
  checkpoint.
- Notifier: the consumer reconnects by itself. Commits attempted during the outage fail (the
  script catches and prints the error), so a message handled just before the outage may be handled
  again afterwards: at-least-once.
- Settings that control this: `message.timeout.ms` and `queue.buffering.max.messages` on the producer,
  the Kafka options passed by Spark (`kafka.*`, `failOnDataLoss`), `session.timeout.ms` and
  `max.poll.interval.ms` on the consumer. With one broker and `replication.factor=1`, everything stops
  while the broker is down (the KRaft controller runs in the same process).

## E.4 Burst at ten times the normal rate

**Hypothesis (written before the run).** The bottleneck is Spark, not Kafka. A single broker can take
thousands of messages per second, but each Spark micro-batch has to parse and deduplicate, join with the
catalog, write Parquet files and publish to Kafka three times (alerts, KPIs, DLQ), and we run it
on one machine with 6 shuffle partitions. At 200 events per second the batches should take longer
and longer, and the delay between event time and alert time should grow. The first thing to change
would be the parameters that control the work per batch (`maxOffsetsPerTrigger`, the trigger interval and
`spark.sql.shuffle.partitions`) before adding partitions.

## E.4 Observation

We ran the simulator at 200 events/s (ten times the normal 20 events/s, run 1181e8e9) for about 100 seconds. Kafka took the burst without trouble: 19805 messages delivered, 0 failed, 0 left in the producer queue. We then polled the Parquet output every 20 seconds (exp4_catchup.txt). At the first poll (21:52:13) the output already contained sequence numbers up to 19600, and the value did not change over the six polls, so Spark had no backlog left at that point. 560 numbers are missing (2.9 %, longest hole of 2), the same level as without any failure (E.1), and the gap between the 19805 delivered messages and the 19600 distinct sequence numbers is the 1 % of duplicates generated by the simulator (about 205 messages).

Our hypothesis was that the limit would be Spark and not Kafka. The first half is only partly confirmed: Kafka was clearly not the limit, but in this test Spark was not either, since it processed ten times the normal load without a lasting backlog. We did not measure the backlog during the burst itself, only its state after it, so we cannot give a latency. The Spark UI (exp4_spark_ui.png, taken about three minutes after the restart of the job, around the burst) shows a processing rate of 233 to 253 events/s per query, with the four queries running in the same local Spark application on one machine. We read this as an order of magnitude for the ceiling of this setup: a sustained rate clearly above 250 events/s would start to build a backlog. This is an inference from the processing rate, we did not run such a test.

## E.5 Performance study (limited)

We did not have the time to run a proper comparison of several configurations, so we do not give a comparison table. What we measured: the job handles 200 events/s with no lasting backlog, and the Spark UI processing rate is about 250 events/s per query on our machine (local mode, four queries, shuffle partitions at 6, at most 5000 offsets per micro-batch). What we did not measure: the end-to-end latency between the event time and the alert, and the effect of the number of shuffle partitions, of the trigger interval or of maxOffsetsPerTrigger. If the input rate had to go above about 250 events/s, these three settings are where we would start, one at a time, while watching the processing rate in the Spark UI, but we have not tested them. For the load of this project (20 events/s, bursts of 200) the current configuration is enough.
