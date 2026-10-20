"""
fleet_daily_pipeline (part D.1): one run per day, for the logical date `ds` (YYYY-MM-DD).

wait_for_files -> check_topics -> quality_job -> read_verdict
    read_verdict --passed--> report_job -> publish_report ----------\
    read_verdict --failed--> publish_quality_alert -----------------+-> join
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REQUIRED_TOPICS = [
    "vehicle_events", "fleet_catalog", "alerts",
    "region_kpis_1min", "vehicle_events_dlq", "daily_reports",
]

# TEMPORARY: stand-ins for the Spark batch jobs of part C (same --date argument, same output
# files as docs/contracts.md). Once the real jobs exist, point RUNNER to .venv/bin/spark-submit
# and the two scripts to spark_jobs/quality_job.py and spark_jobs/report_job.py.
RUNNER = str(PROJECT_ROOT / ".venv" / "bin" / "python")
QUALITY_JOB = str(PROJECT_ROOT / "airflow_dags" / "stubs" / "quality_job_stub.py")
REPORT_JOB = str(PROJECT_ROOT / "airflow_dags" / "stubs" / "report_job_stub.py")


def _day_files_exist(ds: str, **_) -> bool:
    """Sensor condition: at least one Parquet file exists for the day."""
    folder = DATA_DIR / "parquet" / "events" / f"event_date={ds}"
    return folder.is_dir() and any(folder.rglob("*.parquet"))


def _publish(topic: str, key: str, value: str) -> None:
    """Publish one message and fail the task if Kafka does not confirm it."""
    from confluent_kafka import Producer

    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"Publishing to {topic} failed: {errors or 'timeout'}")


@dag(
    dag_id="fleet_daily_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 19),
    catchup=False,  # do not auto-run every past day when the DAG is first enabled (see D.3)
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["capstone"],
)
def fleet_daily_pipeline():
    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=_day_files_exist,
        mode="reschedule",  # frees the worker slot between two checks
        poke_interval=60,
        timeout=6 * 60 * 60,
    )

    @task(retries=5, retry_delay=timedelta(seconds=30))
    def check_topics():
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        existing = set(admin.list_topics(timeout=10).topics)
        missing = [t for t in REQUIRED_TOPICS if t not in existing]
        if missing:
            raise RuntimeError(f"Missing Kafka topics: {missing}")

    quality_job = BashOperator(
        task_id="quality_job",
        bash_command=f"{RUNNER} {QUALITY_JOB} --date " + "{{ ds }}",
    )

    @task.branch
    def read_verdict(ds=None):
        verdict = json.loads((DATA_DIR / "quality" / f"verdict_{ds}.json").read_text())
        return "report_job" if verdict["passed"] else "publish_quality_alert"

    report_job = BashOperator(
        task_id="report_job",
        bash_command=f"{RUNNER} {REPORT_JOB} --date " + "{{ ds }}",
    )

    @task
    def publish_report(ds=None):
        report = (DATA_DIR / "reports" / f"report_{ds}.json").read_text()
        _publish("daily_reports", key=ds, value=report)

    @task
    def publish_quality_alert(ds=None):
        verdict = json.loads((DATA_DIR / "quality" / f"verdict_{ds}.json").read_text())
        alert = {
            "alert_id": f"{ds}:data_quality_failed",
            "alert_type": "data_quality_failed",
            "vehicle_id": None,
            "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "details": {"date": ds, "reasons": verdict.get("reasons", [])},
        }
        _publish("alerts", key="data_quality_failed", value=json.dumps(alert))

    join = EmptyOperator(task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    topics_ok = check_topics()
    branch = read_verdict()
    report_published = publish_report()
    alert_published = publish_quality_alert()

    wait_for_files >> topics_ok >> quality_job >> branch
    branch >> report_job >> report_published >> join
    branch >> alert_published >> join


fleet_daily_pipeline()
