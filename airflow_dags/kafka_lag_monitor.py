"""
kafka_lag_monitor (part D.2): every 5 minutes, measure the lag of the `alerts-notifier` consumer
group on the `alerts` topic and publish a `consumer_lag_high` alert for each partition whose lag
is above LAG_THRESHOLD.

The alert_id contains the run's logical date (see docs/contracts.md), so a retry or a manual
re-run of the same run never publishes the same alert twice.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from kafka_helpers import consumer_lag, publish_once

GROUP = "alerts-notifier"
TOPIC = "alerts"
LAG_THRESHOLD = int(os.environ.get("LAG_THRESHOLD", "10"))  # messages


def build_alerts(lag: dict[int, int], run_date: str, threshold: int, now: str) -> list[dict]:
    """One alert per partition whose lag is strictly above the threshold."""
    return [
        {
            "alert_id": f"{GROUP}:{partition}:{run_date}:consumer_lag_high",
            "alert_type": "consumer_lag_high",
            "vehicle_id": None,
            "event_time": now,
            "details": {"group": GROUP, "topic": TOPIC, "partition": partition,
                        "lag": value, "threshold": threshold},
        }
        for partition, value in sorted(lag.items()) if value > threshold
    ]


@dag(
    dag_id="kafka_lag_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 19),
    catchup=False,  # a lag measured in the past is meaningless: only the current state matters
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["capstone"],
)
def kafka_lag_monitor():
    @task
    def measure_and_alert(logical_date=None):
        lag = consumer_lag(GROUP, TOPIC)
        print(f"Lag of {GROUP} on {TOPIC}: {lag} (threshold {LAG_THRESHOLD})")
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        run_date = logical_date.strftime("%Y-%m-%dT%H:%M:%S")
        alerts = build_alerts(lag, run_date, LAG_THRESHOLD, now)
        if not alerts:
            print("Lag below threshold: no alert")
        for alert in alerts:
            publish_once(
                "alerts", key="consumer_lag_high", value=json.dumps(alert),
                matches=lambda _k, v, a=alert["alert_id"]: _same_id(v, a),
            )

    measure_and_alert()


def _same_id(value: str, alert_id: str) -> bool:
    try:
        return json.loads(value).get("alert_id") == alert_id
    except (ValueError, AttributeError):
        return False


kafka_lag_monitor()
