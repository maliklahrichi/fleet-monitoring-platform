import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ============================================================
# 1. ENVIRONMENT CONFIGURATION
# ============================================================

# Project root: parent of the consumers directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load local environment variables from the project root
load_dotenv(PROJECT_ROOT / ".env", override=True)

hadoop_home = os.getenv("HADOOP_HOME")

if hadoop_home:
    hadoop_home = str(Path(hadoop_home).expanduser().resolve())

    if not (Path(hadoop_home) / "bin" / "winutils.exe").is_file():
        raise FileNotFoundError(
            f"winutils.exe not found in {hadoop_home}\\bin"
        )

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home

# Configure Java from .env
java_home = os.getenv("JAVA_HOME")

if java_home:
    java_home = str(Path(java_home).expanduser().resolve())
    java_exe = Path(java_home) / "bin" / "java.exe"

    if not java_exe.is_file():
        raise FileNotFoundError(
            f"JAVA_HOME is invalid: {java_home}\n"
            "Check the JAVA_HOME value in your .env file."
        )

    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = (
        str(Path(java_home) / "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
    )

# Use the Python interpreter running this script
python_executable = sys.executable

os.environ["PYSPARK_PYTHON"] = python_executable
os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

SOURCE_TOPIC = os.getenv(
    "VEHICLE_EVENTS_TOPIC",
    "vehicle_events"
)

DLQ_TOPIC = os.getenv(
    "VEHICLE_EVENTS_DLQ_TOPIC",
    "vehicle_events_dlq"
)

# Checkpoint directory (absolute or project-relative)
checkpoint_path = Path(
    os.getenv(
        "SPARK_CHECKPOINT_DIR",
        "checkpoints/query1-dlq"
    )
).expanduser()

if not checkpoint_path.is_absolute():
    checkpoint_path = PROJECT_ROOT / checkpoint_path

checkpoint_path = checkpoint_path.resolve()
checkpoint_path.mkdir(parents=True, exist_ok=True)

print("Python:", python_executable)
print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
print("SPARK_HOME:", os.environ.get("SPARK_HOME", "Not set"))
print("Kafka broker:", KAFKA_BOOTSTRAP_SERVERS)
print("Source topic:", SOURCE_TOPIC)
print("DLQ topic:", DLQ_TOPIC)
print("Checkpoint:", checkpoint_path)

# ============================================================
# 2. PYSPARK IMPORTS
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    lit,
    when,
    to_json,
    struct
)
from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    BooleanType
)

# ============================================================
# 3. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Capstone-Query1-ValidationDLQ")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 4. EVENT SCHEMA
# ============================================================

schema = (
    StructType()
    .add("event_id", StringType())
    .add("vehicle_id", StringType())
    .add("event_time", StringType())
    .add("latitude", DoubleType())
    .add("longitude", DoubleType())
    .add("speed_kmh", DoubleType())
    .add("fuel_level_pct", DoubleType())
    .add("harsh_braking", BooleanType())
)

# ============================================================
# 5. READ
# ============================================================