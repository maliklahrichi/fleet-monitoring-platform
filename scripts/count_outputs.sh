#!/usr/bin/env bash
# Counts what the pipeline has produced so far: Parquet rows, alerts and DLQ messages.
cd "$(dirname "$0")/.."
python3 - << 'PY'
import glob
import pandas as pd
files = glob.glob("data/parquet/events/**/*.parquet", recursive=True)
if files:
    df = pd.concat(pd.read_parquet(f) for f in files)
    print(f"parquet rows: {len(df)}  distinct event_id: {df.event_id.nunique()}  files: {len(files)}")
else:
    print("parquet rows: 0")
PY
count() {
  docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --from-beginning --timeout-ms 4000 2>/dev/null
}
alerts=$(count alerts | grep -o '"alert_id":"[^"]*"')
echo "alerts total: $(echo "$alerts" | grep -c .)  distinct alert_id: $(echo "$alerts" | sort -u | grep -c .)"
echo "dlq messages: $(count vehicle_events_dlq | grep -c .)"
