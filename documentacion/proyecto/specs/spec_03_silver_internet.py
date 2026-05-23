"""Generates 03_silver_internet.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/03_silver_internet.ipynb"

cells = [
    md("""
# 03 — Silver: Internet Fijo (MinTIC)

Accesos a internet fijo por municipio, año, trimestre, proveedor y tecnología.

## FILTROS APLICADOS

| # | Filtro | Justificación |
|---|---|---|
| F1 | `NUM_ACCESOS IS NOT NULL AND NUM_ACCESOS >= 0` | Registros sin accesos o con valores negativos son errores del reporte trimestral MinTIC; no aportan información medible. |
| F2 | `VELOCIDAD_BAJADA > 0` | Velocidad de bajada 0 Mbps es físicamente imposible para un servicio activo; identifica registros malformados o de servicios suspendidos no marcados como tal. |

## TRANSFORMACIONES APLICADAS

| # | Transformación | Justificación |
|---|---|---|
| T1 | Renombrar `AÑO` → `ANO`, `No DE ACCESOS` → `NUM_ACCESOS` | El carácter `Ñ` y los espacios en nombres de columna rompen selección por nombre en Spark/HDFS. |
| T2 | Cast `ANO`, `TRIMESTRE` → IntegerType ; `NUM_ACCESOS` → LongType | Bronze los preserva como string; sin tipado numérico no se pueden agregar (sum, avg). |
| T3 | Parsear `VELOCIDAD_BAJADA`/`VELOCIDAD_SUBIDA`: `regexp_replace(',', '.')` → DoubleType | Los valores vienen con coma decimal (locale es-CO, ej. `"8,00"`); cast directo a double da null. |
| T4 | `lpad(COD_DEPARTAMENTO, 2, '0')` → `COD_DEPTO`; `lpad(COD_MUNICIPIO, 5, '0')` → `COD_MPIO` | Códigos DANE son claves estándar de 2 y 5 dígitos. Sin zero-padding el join contra ICFES/SISBEN/MEN falla (códigos como `5` vs `05`). |
| T5 | Normalizar `DEPARTAMENTO`, `MUNICIPIO`, `PROVEEDOR`, `TECNOLOGIA`: `UPPER` + `translate` de acentos | Evita duplicación lógica de categorías por diferencias de mayúscula/minúscula o acentos (ej. `Antioquia` vs `ANTIOQUIA` vs `ANTIOQUÍA`). |
| T6 | Selección de columnas finales y `partitionBy(ANO)` al escribir | Reduce I/O en consultas filtradas por año (3-5× más rápido). |
"""),

    md("## 1. Setup y carga"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType

spark = get_spark("Entrega2-Silver-Internet", executor_memory="3g", driver_memory="2g", cores=2)
df = spark.read.parquet(P.BRONZE_PQ_INTERNET)
print(f"Filas: {df.count():,}   Cols: {len(df.columns)}")
df.printSchema()
df.show(3, truncate=False)
"""),

    md("## 2. Renombres y tipos"),
    code("""
ACCENTS_FROM = "ÁÉÍÓÚÜÑáéíóúüñ"
ACCENTS_TO   = "AEIOUUNaeiouun"

df_s = (
    df
    # Renombrar columna molesta
    .withColumnRenamed("AÑO", "ANO")
    .withColumnRenamed("No DE ACCESOS", "NUM_ACCESOS")
    # Tipos
    .withColumn("ANO", F.col("ANO").cast(IntegerType()))
    .withColumn("TRIMESTRE", F.col("TRIMESTRE").cast(IntegerType()))
    .withColumn("NUM_ACCESOS", F.col("NUM_ACCESOS").cast(LongType()))
    # Velocidades: coma decimal → punto
    .withColumn("VELOCIDAD_BAJADA", F.regexp_replace(F.col("VELOCIDAD_BAJADA"), ",", ".").cast(DoubleType()))
    .withColumn("VELOCIDAD_SUBIDA", F.regexp_replace(F.col("VELOCIDAD_SUBIDA"), ",", ".").cast(DoubleType()))
    # Códigos geográficos
    .withColumn("COD_DEPTO", F.lpad(F.col("COD_DEPARTAMENTO"), 2, "0"))
    .withColumn("COD_MPIO",  F.lpad(F.col("COD_MUNICIPIO"), 5, "0"))
    # Normalización de nombres
    .withColumn("DEPARTAMENTO", F.upper(F.translate(F.col("DEPARTAMENTO"), ACCENTS_FROM, ACCENTS_TO)))
    .withColumn("MUNICIPIO",    F.upper(F.translate(F.col("MUNICIPIO"),    ACCENTS_FROM, ACCENTS_TO)))
    .withColumn("PROVEEDOR",    F.upper(F.translate(F.col("PROVEEDOR"),    ACCENTS_FROM, ACCENTS_TO)))
    .withColumn("SEGMENTO",     F.upper(F.col("SEGMENTO")))
    .withColumn("TECNOLOGIA",   F.upper(F.translate(F.col("TECNOLOGIA"),   ACCENTS_FROM, ACCENTS_TO)))
)
df_s.printSchema()
df_s.select("ANO","TRIMESTRE","COD_MPIO","MUNICIPIO","TECNOLOGIA","VELOCIDAD_BAJADA","NUM_ACCESOS").show(5, truncate=False)
"""),

    md("## 3. Calidad: nulos por columna"),
    code("""
nulos = df_s.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_s.columns
]).toPandas().T
nulos.columns = ["nulls"]
nulos["pct"] = (nulos["nulls"] / df_s.count() * 100).round(2)
nulos.sort_values("nulls", ascending=False)
"""),

    md("## 4. Aplicación de filtros (F1 + F2)"),
    code("""
n0 = df_s.count()
print(f"Filas antes de filtros: {n0:,}")

# F1 — accesos válidos (no nulos, no negativos)
df_s = df_s.filter(F.col("NUM_ACCESOS").isNotNull() & (F.col("NUM_ACCESOS") >= 0))
n1 = df_s.count()
print(f"  Tras F1 (NUM_ACCESOS válido)              : {n1:>10,}  (eliminó {n0-n1:,})")

# F2 — velocidad de bajada estrictamente positiva
df_s = df_s.filter(F.col("VELOCIDAD_BAJADA").isNotNull() & (F.col("VELOCIDAD_BAJADA") > 0))
n2 = df_s.count()
print(f"  Tras F2 (VELOCIDAD_BAJADA > 0)            : {n2:>10,}  (eliminó {n1-n2:,})")
print(f"Total eliminado por filtros: {n0-n2:,} ({100*(n0-n2)/n0:.2f}%)")
"""),

    md("## 5. Escribir Silver Parquet (particionado por ANO)"),
    code("""
import time
COLS_FINAL = [
    "ANO","TRIMESTRE","PROVEEDOR",
    "COD_DEPTO","DEPARTAMENTO",
    "COD_MPIO","MUNICIPIO",
    "SEGMENTO","TECNOLOGIA",
    "VELOCIDAD_BAJADA","VELOCIDAD_SUBIDA",
    "NUM_ACCESOS",
]
t0 = time.time()
(df_s.select(*COLS_FINAL).write
    .mode("overwrite")
    .partitionBy("ANO")
    .option("compression","snappy")
    .parquet(P.SILVER_INTERNET))
print(f"Escrito en {time.time()-t0:.1f}s")
"""),

    md("## 6. Verificación"),
    code("""
sv = spark.read.parquet(P.SILVER_INTERNET)
print(f"Silver rows: {sv.count():,}")
sv.groupBy("ANO").agg(F.sum("NUM_ACCESOS").alias("total_accesos"),
                     F.countDistinct("COD_MPIO").alias("municipios_distintos")) \\
  .orderBy("ANO").show()
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
