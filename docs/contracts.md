# Contracts between the parts (proposal to be validated by the team)

These formats are shared by several parts (Spark, notifier, Airflow). If they change, the others must be told.

## 1. Alerts – topic `alerts`

Message key: `vehicle_id` (otherwise the value of `alert_type`). Value: JSON.

| Field | Description |
|---|---|
| alert_id | deterministic identifier: lets consumers spot a duplicate after a replay |
| alert_type | point_alert, sustained_speeding, data_quality_failed or consumer_lag_high |
| vehicle_id | vehicle concerned, null for data_quality_failed and consumer_lag_high |
| event_time | ISO 8601 UTC timestamp with milliseconds (triggering event, or alert creation time for Airflow) |
| details | JSON object specific to each type (table below) |

| alert_type | alert_id | details | Produced by |
|---|---|---|---|
| point_alert | `<event_id>:point_alert` | reason (speeding or harsh_braking), speed_kmh | Spark, query 1 |
| sustained_speeding | `<vehicle_id>:<window_start>:sustained_speeding` | window_start, window_end, mean_speed_kmh, n_events | Spark, query 3 |
| data_quality_failed | `<date>:data_quality_failed` | date, reasons (list) | Airflow, fleet_daily_pipeline |
| consumer_lag_high | `<group>:<partition>:<run_logical_date>:consumer_lag_high` | group, partition, lag, threshold | Airflow, kafka_lag_monitor |

## 2. Parquet files written by Spark (query 1)

Path: `data/parquet/events/event_date=YYYY-MM-DD/batch_id=N/`

The Airflow sensor waits until the folder `event_date=<day>` exists and contains files. The `data/` folder must not be versioned in Git.

## 3. Quality verdict (part C.1, read by Airflow)

File: `data/quality/verdict_YYYY-MM-DD.json`

```json
{
  "date": "2026-10-20",
  "passed": true,
  "reasons": [],
  "metrics": {"rows": 0, "duplicate_event_ids": 0, "unknown_region_pct": 0.0}
}
```

## 4. Daily report (part C.2, published to `daily_reports`)

File: `data/reports/report_YYYY-MM-DD.json`. Kafka message: key = date (YYYY-MM-DD), value = content of the file.
