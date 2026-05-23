"""SparkSession factory + path constants shared by every notebook.

Importable from a notebook running on the cluster:
    from common_spark import get_spark, P
    spark = get_spark("ProyectoBrecha-Bronze")
"""
from __future__ import annotations
from pyspark.sql import SparkSession

SPARK_MASTER = "spark://spark-master:7077"
HDFS_NAMENODE = "hdfs://10.43.97.164:9000"


class P:
    """All HDFS paths the project uses, kept in one place to avoid typos."""

    # --- Bronze (raw CSVs in HDFS, ASCII-renamed for safety) ---
    BRONZE_CSV_ICFES = "/usr/proyecto/bronze/icfes/Resultados_unicos_Saber_11_20260519.csv"
    BRONZE_CSV_INTERNET = "/usr/proyecto/bronze/internet/Internet_Fijo_Accesos_por_tecnologia_y_segmento_20260407.csv"
    BRONZE_CSV_SISBEN = "/usr/proyecto/bronze/sisben/DNP_Sisben_Personas_20260519.csv"
    BRONZE_CSV_MEN = "/usr/proyecto/bronze/men/MEN_Estadisticas_Educacion_Municipio_20260519.csv"

    # --- Bronze Parquet (we create these in notebook 01) ---
    BRONZE_PQ_ICFES = "/usr/proyecto/bronze_parquet/icfes"
    BRONZE_PQ_INTERNET = "/usr/proyecto/bronze_parquet/internet"
    BRONZE_PQ_SISBEN = "/usr/proyecto/bronze_parquet/sisben"
    BRONZE_PQ_MEN = "/usr/proyecto/bronze_parquet/men"

    # --- Silver (cleaned, normalized) ---
    SILVER_ICFES = "/usr/proyecto/silver/icfes"
    SILVER_INTERNET = "/usr/proyecto/silver/internet"
    SILVER_SISBEN_MPIO = "/usr/proyecto/silver/sisben_municipal"
    SILVER_MEN = "/usr/proyecto/silver/men"

    # --- Gold (analytical tables) ---
    GOLD_PANEL_MUNICIPAL = "/usr/proyecto/gold/panel_municipal"
    GOLD_PANEL_ESTUDIANTE = "/usr/proyecto/gold/panel_estudiante"


def get_spark(app_name: str = "ProyectoBrechaDigital",
              executor_memory: str = "2g",
              driver_memory: str = "2g",
              cores: int = 2) -> SparkSession:
    """Build a SparkSession pointing at the cluster master and HDFS namenode."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.cores", str(cores))
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
