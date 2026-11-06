
import argparse
import json
import time
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EVENTS_DIR = PROJECT_ROOT / "data" / "parquet" / "events"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"


def baseline(df: DataFrame) -> DataFrame:
    return df.groupBy("vehicle_id").agg(F.size(F.collect_set("event_id")).alias("unique_events"))


def salted(df: DataFrame, n_salts: int) -> DataFrame:
    partial = (df
               .withColumn("salt", F.pmod(F.hash("event_id"), F.lit(n_salts)))
               .groupBy("vehicle_id", "salt")
               .agg(F.size(F.collect_set("event_id")).alias("partial_unique")))
    return partial.groupBy("vehicle_id").agg(F.sum("partial_unique").alias("unique_events"))


def stage_stats(spark: SparkSession, group: str) -> dict:
    """Interroge l'API REST du Spark UI : répartition des tâches du stage le plus chargé."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    stage_ids = [s for j in sc.statusTracker().getJobIdsForGroup(group)
                 for s in sc.statusTracker().getJobInfo(j).stageIds]
    best = None
    for sid in stage_ids:
        try:
            for st in json.load(opener.open(f"{base}/stages/{sid}")):
                if st["status"] == "COMPLETE" and (best is None or
                        st["shuffleReadBytes"] > best["shuffleReadBytes"]):
                    best = st
        except Exception:
            continue
    if best is None:
        return {}
    q = json.load(opener.open(f"{base}/stages/{best['stageId']}/{best['attemptId']}"
                              "/taskSummary?quantiles=0.5,1.0"))
    t_med, t_max = q["executorRunTime"]
    b_med, b_max = q["shuffleReadMetrics"]["readBytes"]
    return {"stage_id": best["stageId"], "tasks": best["numTasks"],
            "task_ms_median": t_med, "task_ms_max": t_max,
            "shuffle_read_kb_median": round(b_med / 1024), "shuffle_read_kb_max": round(b_max / 1024)}


def run(spark: SparkSession, name: str, df: DataFrame, aqe: bool) -> tuple:
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
    spark.sparkContext.setJobGroup(name, name)
    start = time.time()
    result = {r["vehicle_id"]: r["unique_events"] for r in df.collect()}
    seconds = round(time.time() - start, 2)
    stats = stage_stats(spark, name)
    print(f"{name:<16} {seconds:>6} s  {stats}")
    return result, {"variant": name, "aqe": aqe, "seconds": seconds, **stats}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--repeat", type=int, default=1, help="facteur de multiplication des données")
    parser.add_argument("--salt", type=int, default=8, help="nombre de morceaux par clé")
    parser.add_argument("--wait", action="store_true", help="garder le Spark UI ouvert à la fin")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName(f"skew-study-{args.date}")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    day = (spark.read.parquet(str(EVENTS_DIR))
           .filter(F.col("event_date") == F.lit(args.date).cast("date"))
           .select("vehicle_id", "event_id"))
    if args.repeat > 1:   # copies avec des event_id différents → même répartition, plus de volume
        day = (day.crossJoin(spark.range(args.repeat).withColumnRenamed("id", "copy"))
                  .withColumn("event_id", F.concat_ws(":", "event_id", "copy"))
                  .drop("copy"))
    day = day.repartition(8).cache()   # données en mémoire : on mesure l'agrégation, pas la lecture
    total = day.count()
    hot = day.groupBy("vehicle_id").count().orderBy(F.desc("count")).first()
    print(f"{total} lignes ; clé la plus fréquente : {hot['vehicle_id']} "
          f"({100 * hot['count'] / total:.1f} %)")

    baseline(day).collect()   # tour d'échauffement (non mesuré) : JVM et cache déjà chauds

    rows, results = [], []
    for name, df, aqe in [("baseline", baseline(day), False),
                          ("baseline+AQE", baseline(day), True),
                          ("salting", salted(day, args.salt), False),
                          ("salting+AQE", salted(day, args.salt), True)]:
        result, row = run(spark, name, df, aqe)
        results.append(result)
        rows.append(row)

    assert all(r == results[0] for r in results), "les 4 variantes doivent donner le même résultat"
    print("Les 4 variantes donnent exactement le même résultat.")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / f"skew_study_{args.date}.json"
    out.write_text(json.dumps({"date": args.date, "rows": total, "repeat": args.repeat,
                               "salts": args.salt, "hot_key": hot["vehicle_id"],
                               "hot_key_pct": round(100 * hot["count"] / total, 1),
                               "variants": rows}, indent=2))
    print(f"Résultats écrits dans {out}")

    if args.wait:
        input(f"Spark UI : {spark.sparkContext.uiWebUrl} - Entrée pour quitter... ")
    spark.stop()


if __name__ == "__main__":
    main()