# Part D - Airflow orchestration (D.1 to D.4)

## DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `fleet_daily_pipeline` | `@daily` | Wait for the day's Parquet files, check Kafka, run the quality job, branch on the verdict, publish the daily report or a `data_quality_failed` alert. |
| `kafka_lag_monitor` | every 5 minutes | Measure the lag of the `alerts-notifier` group on `alerts` and publish a `consumer_lag_high` alert when a partition exceeds a threshold (default 10 messages, `LAG_THRESHOLD`). |

Flow of `fleet_daily_pipeline`:

```
wait_for_files -> check_topics -> quality_job -> read_verdict
    read_verdict -- passed --> report_job -> publish_report ---------\
    read_verdict -- failed --> publish_quality_alert -----------------+-> join
```

## D.3 Logical date, catchup and idempotence

**Logical date versus execution time.** The run labelled `2026-10-19` is the run for the data of
19 October. With `@daily` it starts just after the interval ends, on 20 October at 00:00 UTC. The
logical date (`ds`, the start of the interval) tells a task which day of data it owns; the
execution time is only when it happens to run. Every task uses `ds` (never "today"), so re-running
the run for 19 October on 25 October still processes 19 October.

**`catchup=False`.** When the DAG is first enabled, Airflow does not create one run for every day
since `start_date`. Old days are processed only when we ask for it explicitly with a backfill. This
avoids an uncontrolled burst of runs on a single-worker setup. For `kafka_lag_monitor`, catchup would
be meaningless: a lag measured in the past says nothing about the current state.

**Idempotence.** Re-running a task must leave the world in the same state as running it once:

| Task | Why it can run twice |
|---|---|
| `quality_job`, `report_job` | The Spark/batch jobs overwrite their output file for the date (`verdict_<date>.json`, `report_<date>.json`). |
| `publish_report` | `publish_once` reads the topic first and skips the publication when the same key and the same content are already there. |
| `publish_quality_alert` | The alert is recognised by its `alert_id` (`<date>:data_quality_failed`), because `event_time` differs between runs. |
| `kafka_lag_monitor` | The `alert_id` contains the run's logical date, so a retry of the same run never publishes the same alert twice. |

Why not rely on the idempotent producer alone? Kafka's idempotent producer (`enable.idempotence`)
only removes duplicates caused by *retries inside one producer session* (same producer id and
sequence numbers). A new task run creates a new producer, so Kafka cannot know the message is a
repeat. The check therefore has to be done at application level, with a business key.

**Proof.** (a) `publish_report` was re-run for 2026-10-19 by clearing `quality_job`; its log shows
`Skipped: a matching message is already in daily_reports (key=2026-10-19)` and `daily_reports`
still holds exactly 1 message. (b) `publish_quality_alert` was re-run for the same date and
`alerts` still holds a single `2026-10-19:data_quality_failed` alert. (c) `kafka_lag_monitor` was
run twice with the same logical date: the second run logged `Skipped` for the partition already
alerted and published only the alert for a different partition. Screenshots are in
`report/screenshots/`.

**Limit.** Check-then-publish is not atomic: two copies of the same task running at the same time
could both see an empty topic. It is safe here because `max_active_runs=1` and a task instance
never runs concurrently with itself. A strict guarantee would need a transactional producer or a
deduplicating consumer on `alert_id`.

**Backfill of 7 days.** (To fill in once done.) Command:
`airflow dags backfill -s 2026-10-12 -e 2026-10-18 fleet_daily_pipeline`, after creating a Parquet
file for each day. Result to report: 7 runs, one report per date, no duplicate message per key.

## D.4 Sensor: `poke` versus `reschedule`

`wait_for_files` is a `PythonSensor` in **`reschedule`** mode with `poke_interval=60` and a 6-hour
timeout.

- In `poke` mode the sensor keeps its worker slot for the whole wait and sleeps between checks.
- In `reschedule` mode each check is a short task: if the condition is false the task exits and
  is scheduled again 60 seconds later, so the slot is free in between.

Our setup uses the `SequentialExecutor` with SQLite, i.e. **parallelism 1**: only one task can run
at a time. A `poke` sensor waiting for late files would occupy that single slot, and nothing else
(including `kafka_lag_monitor`) could run until it finished or timed out. In `reschedule` mode the
slot is released between checks. The price is a bit more scheduler and database activity (one task
instance per check), which is negligible at one check per minute.

Retries: `check_topics` has `retries=5` with a 30-second delay because a broker restarting is a
transient failure; the other tasks use the default of 1 retry after 1 minute.
