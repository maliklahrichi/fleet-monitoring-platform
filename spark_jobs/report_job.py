
import argparse
import json
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EVENTS_DIR = PROJECT_ROOT / "data" / "parquet" / "events"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"

SPEEDING_KMH = 120   # même seuil que les alertes ponctuelles (B.4) et les KPI (B.5)
WORST_N = 5


def indicators(df: DataFrame, *keys: str) -> DataFrame:
    """Les 5 indicateurs demandés, regroupés selon `keys`."""
    return (df.groupBy(*keys)
            .agg(F.count("*").alias("events"),
                 F.round(F.avg("speed_kmh"), 2).alias("mean_speed_kmh"),
                 F.max("speed_kmh").alias("max_speed_kmh"),
                 F.sum(F.when(F.col("speed_kmh") > SPEEDING_KMH, 1).otherwise(0))
                  .alias("speeding_events"),
                 F.sum(F.when(F.col("harsh_braking"), 1).otherwise(0))
                  .alias("harsh_braking_events")))


def rows_as_dicts(df: DataFrame, order: list) -> list:
    """Petits résultats (≤ 45 véhicules, 6 régions) : on les ramène sur le driver."""
    return [r.asDict() for r in df.orderBy(*order).collect()]


def scan_metrics(spark: SparkSession) -> list:
    """Lit dans l'API REST du Spark UI ce que les lectures Parquet ont réellement ouvert.

    Mêmes chiffres que l'onglet SQL / DataFrame du Spark UI (nœud "Scan parquet").
    """
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/sql?details=true&length=1000"
    wanted = ("number of files read", "number of partitions read", "size of files read")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    found = []
    try:
        for query in json.load(opener.open(url)):
            for node in query.get("nodes", []):
                if node.get("nodeName", "").startswith("Scan parquet"):
                    found.append({m["name"]: m["value"] for m in node.get("metrics", [])
                                  if m["name"] in wanted})
    except Exception as exc:        # le Spark UI peut être désactivé : ce n'est qu'une preuve
        print(f"(métriques du Spark UI indisponibles : {exc})")
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName(f"report-job-{args.date}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    day = (spark.read.parquet(str(EVENTS_DIR))
           .filter(F.col("event_date") == F.lit(args.date).cast("date")))

    # --- Preuve du partition pruning (C.2) ---
    plan = day._jdf.queryExecution().executedPlan().toString()
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / f"plan_{args.date}.txt").write_text(plan)
    print("Plan physique de la lecture :\n" + plan)

    day = day.cache()   # lu une fois, utilisé pour les 3 calculs

    per_vehicle = indicators(day, "vehicle_id", "region")
    per_region = indicators(day, "region")

    # "Pire" véhicule = le plus d'incidents (excès + freinages brusques), puis la vitesse max.
    worst = (per_vehicle
             .withColumn("incidents", F.col("speeding_events") + F.col("harsh_braking_events"))
             .orderBy(F.col("incidents").desc(), F.col("max_speed_kmh").desc(), "vehicle_id")
             .limit(WORST_N))

    report = {
        "date": args.date,
        "total_events": day.count(),
        "per_region": rows_as_dicts(per_region, ["region"]),
        "per_vehicle": rows_as_dicts(per_vehicle, ["vehicle_id"]),
        "worst_vehicles": [r.asDict() for r in worst.collect()],
    }
    # Pas d'horodatage "généré à" dans le rapport : deux exécutions sur les mêmes données
    # doivent produire EXACTEMENT le même contenu, sinon publish_report (Airflow) croirait
    # à un nouveau rapport et le republierait.
    out = REPORTS_DIR / f"report_{args.date}.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True))
    print(f"Rapport écrit dans {out} ({report['total_events']} événements)")

    total_partitions = sum(1 for _ in EVENTS_DIR.glob("event_date=*/batch_id=*"))
    for scan in filter(None, scan_metrics(spark)):     # les lectures servies par le cache n'ont pas de métriques
        print(f"Lecture Parquet : {scan}  (dossiers event_date/batch_id existants : {total_partitions})")
    spark.stop()


if __name__ == "__main__":
    main()