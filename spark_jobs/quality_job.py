
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EVENTS_DIR = PROJECT_ROOT / "data" / "parquet" / "events"
QUALITY_DIR = PROJECT_ROOT / "data" / "quality"

# Seuils de la porte de qualité (justification dans le rapport, partie C.1)
MIN_ROWS = 1000             # < 1000 lignes : le pipeline n'a quasiment pas tourné ce jour-là
MAX_DUPLICATE_PCT = 0.5     # le simulateur envoie 1 % de doublons ; Spark doit en retirer l'essentiel
MAX_UNKNOWN_PCT = 2.0       # normalement 0 % ; au-delà, catalogue incomplet ou jointure cassée


def read_day(spark: SparkSession, date: str):
    """Lit uniquement le dossier event_date=<date> (partition pruning)."""
    if not EVENTS_DIR.is_dir():
        return None
    return (spark.read.parquet(str(EVENTS_DIR))
            .filter(F.col("event_date") == F.lit(date).cast("date")))


def compute_metrics(day) -> dict:
    """Les 3 mesures en UNE seule lecture des données (une seule agrégation)."""
    if day is None:
        return {"rows": 0, "duplicate_event_ids": 0, "duplicate_pct": 0.0,
                "unknown_region_pct": 0.0}
    row = day.agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("distinct_ids"),
        F.sum(F.when(F.col("region") == "UNKNOWN", 1).otherwise(0)).alias("unknown"),
    ).first()
    rows = row["rows"]
    duplicates = rows - row["distinct_ids"]          # lignes en trop (copies d'un même event_id)
    unknown = row["unknown"] or 0
    return {
        "rows": rows,
        "duplicate_event_ids": duplicates,
        "duplicate_pct": round(100 * duplicates / rows, 3) if rows else 0.0,
        "unknown_region_pct": round(100 * unknown / rows, 3) if rows else 0.0,
    }


def decide(metrics: dict) -> list:
    """Renvoie la liste des raisons d'échec (liste vide = journée validée)."""
    reasons = []
    if metrics["rows"] < MIN_ROWS:
        reasons.append(f"rows {metrics['rows']} < {MIN_ROWS}")
    if metrics["duplicate_pct"] > MAX_DUPLICATE_PCT:
        reasons.append(f"duplicate_pct {metrics['duplicate_pct']} > {MAX_DUPLICATE_PCT}")
    if metrics["unknown_region_pct"] > MAX_UNKNOWN_PCT:
        reasons.append(f"unknown_region_pct {metrics['unknown_region_pct']} > {MAX_UNKNOWN_PCT}")
    return reasons


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (la date logique d'Airflow)")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName(f"quality-job-{args.date}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    metrics = compute_metrics(read_day(spark, args.date))
    reasons = decide(metrics)
    verdict = {
        "date": args.date,
        "passed": not reasons,
        "reasons": reasons,
        "metrics": metrics,
        "thresholds": {"min_rows": MIN_ROWS, "max_duplicate_pct": MAX_DUPLICATE_PCT,
                       "max_unknown_region_pct": MAX_UNKNOWN_PCT},
    }

    # Idempotent : le fichier du jour est simplement réécrit à chaque exécution.
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    out = QUALITY_DIR / f"verdict_{args.date}.json"
    out.write_text(json.dumps(verdict, indent=2))
    print(f"Verdict écrit dans {out} : passed={verdict['passed']} {reasons}")
    spark.stop()


if __name__ == "__main__":
    main()