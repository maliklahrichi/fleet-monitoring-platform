
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# ============================================================
# Configuration
# ============================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

EVENTS_TOPIC = "vehicle_events"
CATALOG_TOPIC = "fleet_catalog"
DLQ_TOPIC = "vehicle_events_dlq"
ALERTS_TOPIC = "alerts"
KPIS_TOPIC = "region_kpis_1min"

PARQUET_DIR = str(PROJECT_ROOT / "data" / "parquet" / "events")      # docs/contracts.md
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"                         # un sous-dossier par requête

TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"   # ex. 2026-10-13T09:16:00.123+00:00
SPEED_MIN, SPEED_MAX = 0, 250                   # vitesses possibles (B.1)
POINT_SPEED_LIMIT = 120                         # alerte ponctuelle (B.4) et "excès" des KPI (B.5)
SUSTAINED_SPEED = 100                           # alerte sustained_speeding (B.6)
SUSTAINED_MIN_EVENTS = 5

# Schéma explicite d'un événement + une colonne qui reçoit le texte brut si le JSON est invalide
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("harsh_braking", BooleanType()),
    StructField("_corrupt_record", StringType()),
])

CATALOG_SCHEMA = StructType([
    StructField("vehicle_id", StringType()),
    StructField("plate", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("home_region", StringType()),
    StructField("max_payload_kg", IntegerType()),
])


# ============================================================
# B.1 - Lecture, parsing et validation
# ============================================================
def read_events(spark: SparkSession) -> DataFrame:
    """Flux brut de vehicle_events : clé et valeur en texte."""
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", EVENTS_TOPIC)
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", 5000)       # taille max d'un micro-batch (étude de perf, partie E)
           .load())
    return raw.select(F.col("key").cast("string").alias("kafka_key"),
                      F.col("value").cast("string").alias("raw_value"))


def parse_and_validate(raw: DataFrame) -> DataFrame:
    """Ajoute les colonnes parsées et une colonne `reason` : NULL si valide, sinon la raison du rejet.

    Piège du NULL : `speed_kmh BETWEEN 0 AND 250` vaut NULL (ni vrai ni faux) si la vitesse
    manque. Un filtre "valide" ET un filtre "invalide" écarteraient tous les deux la ligne,
    qui disparaîtrait. On teste donc TOUJOURS isNull() avant de comparer une valeur, et
    chaque ligne reçoit soit une raison, soit NULL : les deux flux sont complémentaires.
    """
    parsed = (raw
              .withColumn("data", F.from_json("raw_value", EVENT_SCHEMA,
                                              {"columnNameOfCorruptRecord": "_corrupt_record"}))
              .select("kafka_key", "raw_value", "data.*")
              .withColumn("event_ts", F.to_timestamp("event_time", TIME_FORMAT)))

    reason = (F.when(F.col("raw_value").isNull() | F.col("_corrupt_record").isNotNull(),
                     F.lit("broken_json"))
              .when(F.col("event_id").isNull(), F.lit("missing_field:event_id"))
              .when(F.col("vehicle_id").isNull(), F.lit("missing_field:vehicle_id"))
              .when(F.col("event_time").isNull(), F.lit("missing_field:event_time"))
              .when(F.col("speed_kmh").isNull(), F.lit("missing_field:speed_kmh"))
              .when(F.col("event_ts").isNull(), F.lit("bad_timestamp"))
              .when((F.col("speed_kmh") < SPEED_MIN) | (F.col("speed_kmh") > SPEED_MAX),
                    F.lit("speed_out_of_range")))
              # aucune condition remplie → reason = NULL → message valide
    return parsed.withColumn("reason", reason)


def to_dlq(checked: DataFrame) -> DataFrame:
    """Messages invalides au format Kafka (key, value) : le message brut + la raison."""
    return (checked
            .filter(F.col("reason").isNotNull())
            .select(F.col("kafka_key").alias("key"),
                    F.to_json(F.struct(
                        F.col("raw_value"),
                        F.col("reason"),
                        F.date_format(F.current_timestamp(), TIME_FORMAT).alias("rejected_at"),
                    )).alias("value")))


def valid_events(checked: DataFrame) -> DataFrame:
    """Messages valides, avec event_time devenu un vrai timestamp."""
    return (checked
            .filter(F.col("reason").isNull())
            .select("event_id", "vehicle_id",
                    F.col("event_ts").alias("event_time"),
                    "latitude", "longitude", "speed_kmh", "fuel_level_pct",
                    F.coalesce(F.col("harsh_braking"), F.lit(False)).alias("harsh_braking")))


# ============================================================
# B.2 - Déduplication
# ============================================================
def deduplicate(events: DataFrame) -> DataFrame:
    """Supprime les doublons d'event_id arrivant à moins de 2 minutes d'écart (event time).

    L'état est borné : Spark oublie un event_id dès que le watermark (max event_time vu - 2 min)
    a dépassé son event_time. Un doublon plus tardif n'est plus reconnu.
    """
    return (events
            .withWatermark("event_time", "2 minutes")
            .dropDuplicatesWithinWatermark(["event_id"]))


# ============================================================
# B.3 - Enrichissement avec le catalogue
# ============================================================
def load_catalog(spark: SparkSession) -> DataFrame:
    """Lit fleet_catalog UNE FOIS (lecture batch) et garde la dernière version de chaque véhicule."""
    raw = (spark.read
           .format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", CATALOG_TOPIC)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load()
           .select(F.col("key").cast("string").alias("key"),
                   F.col("value").cast("string").alias("value"), "offset"))
    return latest_catalog(raw)


def latest_catalog(raw: DataFrame) -> DataFrame:
    """1 ligne par véhicule : le message de plus grand offset (même clé → même partition)."""
    newest_first = Window.partitionBy("key").orderBy(F.col("offset").desc())
    catalog = (raw
               .withColumn("rank", F.row_number().over(newest_first))
               .filter((F.col("rank") == 1) & F.col("value").isNotNull())   # value NULL = tombstone
               .select(F.from_json("value", CATALOG_SCHEMA).alias("c"))
               .select(F.col("c.vehicle_id").alias("vehicle_id"),
                       F.col("c.home_region").alias("region"),
                       F.col("c.plate").alias("plate"),
                       F.col("c.vehicle_type").alias("vehicle_type"),
                       F.col("c.max_payload_kg").alias("max_payload_kg"))
               .cache())   # photo figée au démarrage : les changements du catalogue ne sont pas vus
    print(f"Catalogue chargé : {catalog.count()} véhicules")
    return catalog


def enrich(events: DataFrame, catalog: DataFrame) -> DataFrame:
    """Jointure stream-static LEFT + broadcast : aucun événement ne disparaît."""
    return (events
            .join(F.broadcast(catalog), on="vehicle_id", how="left")
            .withColumn("region", F.coalesce(F.col("region"), F.lit("UNKNOWN"))))


# ============================================================
# B.4 - Query 1 : Parquet + alertes ponctuelles (foreachBatch)
# ============================================================
def point_alerts(batch: DataFrame) -> DataFrame:
    """Alertes ponctuelles au format de docs/contracts.md, prêtes pour Kafka (key, value)."""
    alerts = (batch
              .filter((F.col("speed_kmh") > POINT_SPEED_LIMIT) | F.col("harsh_braking"))
              .withColumn("why", F.when(F.col("speed_kmh") > POINT_SPEED_LIMIT, "speeding")
                                  .otherwise("harsh_braking")))
    return alerts.select(
        F.col("vehicle_id").alias("key"),
        F.to_json(F.struct(
            F.concat(F.col("event_id"), F.lit(":point_alert")).alias("alert_id"),
            F.lit("point_alert").alias("alert_type"),
            F.col("vehicle_id"),
            F.date_format("event_time", TIME_FORMAT).alias("event_time"),
            F.struct(F.col("why").alias("reason"), F.col("speed_kmh")).alias("details"),
        )).alias("value"))


def write_parquet(batch: DataFrame, batch_id: int) -> None:
    """Écrit le micro-batch dans event_date=.../batch_id=.../

    Idempotent : mode overwrite + partitionOverwriteMode=dynamic ne remplace QUE les partitions
    présentes dans ce batch. Si le batch N est rejoué après un crash, il réécrit exactement son
    dossier batch_id=N au lieu d'ajouter une 2e copie.
    """
    (batch
     .withColumn("event_date", F.to_date("event_time"))
     .withColumn("batch_id", F.lit(batch_id))
     .write
     .mode("overwrite")
     .option("partitionOverwriteMode", "dynamic")
     .partitionBy("event_date", "batch_id")
     .parquet(PARQUET_DIR))


def publish_to_kafka(df: DataFrame, topic: str) -> None:
    """Écriture batch vers Kafka. PAS idempotente : un batch rejoué republie ses messages."""
    (df.write
     .format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", topic)
     .save())


def process_batch(batch: DataFrame, batch_id: int) -> None:
    """Appelée par Spark pour chaque micro-batch de la Query 1."""
    batch.persist()           # le batch sert deux fois : on évite de le recalculer
    write_parquet(batch, batch_id)
    publish_to_kafka(point_alerts(batch), ALERTS_TOPIC)
    batch.unpersist()


# ============================================================
# B.5 - Query 2 : KPI par région, fenêtre fixe d'une minute
# ============================================================
def region_kpis(enriched: DataFrame) -> DataFrame:
    kpis = (enriched
            .groupBy(F.window("event_time", "1 minute"), "region")
            .agg(F.count("*").alias("events"),
                 F.round(F.avg("speed_kmh"), 2).alias("mean_speed_kmh"),
                 F.max("speed_kmh").alias("max_speed_kmh"),
                 F.sum(F.when(F.col("speed_kmh") > POINT_SPEED_LIMIT, 1).otherwise(0))
                  .alias("speeding_events")))
    return kpis.select(
        F.col("region").alias("key"),
        F.to_json(F.struct(
            F.date_format("window.start", TIME_FORMAT).alias("window_start"),
            F.date_format("window.end", TIME_FORMAT).alias("window_end"),
            "region", "events", "mean_speed_kmh", "max_speed_kmh", "speeding_events",
        )).alias("value"))


# ============================================================
# B.6 - Query 3 : excès de vitesse prolongé, fenêtre glissante
# ============================================================
def sustained_speeding(enriched: DataFrame) -> DataFrame:
    stats = (enriched
             .groupBy(F.window("event_time", "5 minutes", "1 minute"), "vehicle_id")
             .agg(F.avg("speed_kmh").alias("mean_speed_kmh"),
                  F.count("*").alias("n_events")))
    alerts = stats.filter((F.col("mean_speed_kmh") > SUSTAINED_SPEED)
                          & (F.col("n_events") >= SUSTAINED_MIN_EVENTS))
    start = F.date_format("window.start", TIME_FORMAT)
    return alerts.select(
        F.col("vehicle_id").alias("key"),
        F.to_json(F.struct(
            F.concat_ws(":", "vehicle_id", start, F.lit("sustained_speeding")).alias("alert_id"),
            F.lit("sustained_speeding").alias("alert_type"),
            F.col("vehicle_id"),
            F.date_format("window.end", TIME_FORMAT).alias("event_time"),
            F.struct(start.alias("window_start"),
                     F.date_format("window.end", TIME_FORMAT).alias("window_end"),
                     F.round("mean_speed_kmh", 2).alias("mean_speed_kmh"),
                     F.col("n_events")).alias("details"),
        )).alias("value"))


# ============================================================
# Assemblage : une application, quatre requêtes
# ============================================================
def kafka_sink(df: DataFrame, topic: str, name: str, mode: str):
    return (df.writeStream
            .queryName(name)
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", topic)
            .option("checkpointLocation", str(CHECKPOINT_DIR / name))
            .outputMode(mode)
            .start())


def main() -> None:
    spark = (SparkSession.builder
             .appName("fleet-streaming")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")   # event_date et horodatages en UTC
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    checked = parse_and_validate(read_events(spark))
    catalog = load_catalog(spark)
    enriched = enrich(deduplicate(valid_events(checked)), catalog)

    kafka_sink(to_dlq(checked), DLQ_TOPIC, "dlq", "append")

    (enriched.writeStream
     .queryName("query1")
     .foreachBatch(process_batch)
     .option("checkpointLocation", str(CHECKPOINT_DIR / "query1"))
     .start())

    kafka_sink(region_kpis(enriched), KPIS_TOPIC, "query2", "update")
    kafka_sink(sustained_speeding(enriched), ALERTS_TOPIC, "query3", "append")

    print("4 requêtes démarrées : dlq, query1, query2, query3. Ctrl+C pour arrêter.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()