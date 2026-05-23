"""Generates 02_silver_icfes.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/02_silver_icfes.ipynb"

cells = [
    md("""
# 02 — Silver: ICFES Saber 11

Limpieza y normalización del dataset de resultados Saber 11.

## FILTROS APLICADOS

| # | Filtro | Justificación |
|---|---|---|
| F1 | `PUNT_GLOBAL IS NOT NULL` | El puntaje global es la **variable objetivo del modelo supervisado**; un estudiante sin puntaje no aporta señal y sesga las agregaciones municipales. Elimina ~37% de filas (estudiantes que no presentaron). |
| F2 | `PUNT_GLOBAL > 0` | Puntaje cero indica registro incompleto/inválido (en Saber 11 el mínimo posible al presentar es ~50). Estos registros distorsionan los promedios municipales. |
| F3 | `PUNT_GLOBAL < 500` | El máximo teórico de Saber 11 es 500. Valores superiores son errores de captura o atípicos extremos no admisibles. |

## TRANSFORMACIONES APLICADAS

| # | Transformación | Justificación |
|---|---|---|
| T1 | Cast `PUNT_GLOBAL` → IntegerType | Bronze lo preserva como string; sin tipado no se pueden hacer agregaciones (avg, stddev) ni usar como label en MLlib. |
| T2 | Cast `PUNT_LECTURA_CRITICA`, `PUNT_MATEMATICAS`, `PUNT_C_NATURALES`, `PUNT_SOCIALES_CIUDADANAS`, `PUNT_INGLES` → DoubleType vía `regexp_replace(',', '.')` | Los puntajes por área pueden venir con coma decimal (locale es-CO, ej. `"55,2"`); el cast directo a numérico daría null. |
| T3 | Estandarizar `FAMI_TIENEINTERNET` y `FAMI_TIENECOMPUTADOR` → `UPPER` + `coalesce("SIN INFORMACION")` | Imputación explícita de nulos en categóricas críticas (en lugar de drop) preserva el registro para análisis ML donde el "no responde" es información en sí mismo. |
| T4 | Derivar binarias `TIENE_INTERNET_BIN`, `TIENE_COMPUTADOR_BIN` (Si→1, otro→0) | Requeridas para correlaciones de Pearson, promedios municipales (`pct_internet_icfes`) y features de modelos. |
| T5 | Normalizar nombres geográficos: `UPPER` + `translate` de acentos (`Á→A`, etc.) sobre `COLE_DEPTO_UBICACION` y `COLE_MCPIO_UBICACION` | Evita duplicación lógica por diferencias de capitalización/acento (`BOGOTÁ` vs `BOGOTA`). |
| T6 | `lpad(COLE_COD_DEPTO_UBICACION, 2, '0')` → `COD_DEPTO` ; `lpad(COLE_COD_MCPIO_UBICACION, 5, '0')` → `COD_MPIO` | Códigos DANE son claves de 2 y 5 dígitos. El zero-padding garantiza join correcto contra Internet/SISBEN/MEN. |
| T7 | Derivar `ANO = int(substring(PERIODO, 1, 4))` | El campo `PERIODO` es `YYYYS` (año + semestre, ej. `20194` = 2019-II). Extraer año permite particionar y comparar por cohorte. |
| T8 | Bucketizar `RANGO_PUNT_GLOBAL` con `when`: `<200`=BAJO, `<300`=MEDIO, resto=ALTO | Feature categórica derivada útil para clasificación y análisis exploratorio (Q5). |
"""),

    md("## 1. Setup"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

spark = get_spark("Entrega2-Silver-ICFES", executor_memory="4g", driver_memory="2g", cores=2)
print("App ID:", spark.sparkContext.applicationId)
"""),

    md("## 2. Cargar Bronze ICFES"),
    code("""
icfes_bronze = spark.read.parquet(P.BRONZE_PQ_ICFES)
print(f"Filas bronze: {icfes_bronze.count():,}")
print(f"Columnas    : {len(icfes_bronze.columns)}")
icfes_bronze.printSchema()
"""),

    md("## 3. Filtrar registros sin puntaje global y atípicos"),
    code("""
df = (
    icfes_bronze
    .withColumn("PUNT_GLOBAL", F.col("PUNT_GLOBAL").cast(IntegerType()))
    .filter(F.col("PUNT_GLOBAL").isNotNull())
    .filter(F.col("PUNT_GLOBAL") > 0)
    .filter(F.col("PUNT_GLOBAL") < 500)  # max teórico Saber 11 es 500
)
print(f"Filas tras filtro: {df.count():,}")
"""),

    md("## 4. Tipificar puntajes por área"),
    code("""
# Los puntajes por área pueden llegar como "55" (int) o "55,2" (decimal con coma — locale es-CO).
# Solución segura: cast→string, replace coma por punto, cast→double.
PUNT_COLS = [c for c in df.columns if c.startswith("PUNT_") and c != "PUNT_GLOBAL"]
print("Cols de puntaje por área:", PUNT_COLS)
for c in PUNT_COLS:
    df = df.withColumn(c,
                       F.regexp_replace(F.col(c).cast("string"), ",", ".").cast(DoubleType()))
df.select(["PUNT_GLOBAL"] + PUNT_COLS).describe().show()
"""),

    md("## 5. Normalizar tecnología en el hogar"),
    code("""
# Estandarizar mayúsculas y rellenar nulos
df = df.withColumn("FAMI_TIENEINTERNET",
                   F.upper(F.coalesce(F.col("FAMI_TIENEINTERNET"), F.lit("SIN INFORMACION"))))
df = df.withColumn("FAMI_TIENECOMPUTADOR",
                   F.upper(F.coalesce(F.col("FAMI_TIENECOMPUTADOR"), F.lit("SIN INFORMACION"))))

# Binarias (Si → 1, No/SIN INFORMACION → 0). En Saber 11 los valores son "Si"/"No" originalmente.
df = (df
    .withColumn("TIENE_INTERNET_BIN", F.when(F.col("FAMI_TIENEINTERNET") == "SI", 1).otherwise(0))
    .withColumn("TIENE_COMPUTADOR_BIN", F.when(F.col("FAMI_TIENECOMPUTADOR") == "SI", 1).otherwise(0))
)
df.groupBy("FAMI_TIENEINTERNET", "TIENE_INTERNET_BIN").count().orderBy(F.desc("count")).show(10)
"""),

    md("## 6. Normalizar nombres geográficos"),
    code("""
# UPPER + remover acentos vía translate (ÁÉÍÓÚáéíóúÑñ → AEIOUaeiouNn)
ACCENTS_FROM = "ÁÉÍÓÚÜÑáéíóúüñ"
ACCENTS_TO   = "AEIOUUNaeiouun"

df = (df
    .withColumn("COLE_DEPTO_UBICACION", F.upper(F.translate(F.col("COLE_DEPTO_UBICACION"), ACCENTS_FROM, ACCENTS_TO)))
    .withColumn("COLE_MCPIO_UBICACION", F.upper(F.translate(F.col("COLE_MCPIO_UBICACION"), ACCENTS_FROM, ACCENTS_TO)))
)
# Rellenar códigos DANE a la longitud canónica
df = (df
    .withColumn("COD_DEPTO", F.lpad(F.col("COLE_COD_DEPTO_UBICACION"), 2, "0"))
    .withColumn("COD_MPIO",  F.lpad(F.col("COLE_COD_MCPIO_UBICACION"), 5, "0"))
)
df.select("COLE_DEPTO_UBICACION","COD_DEPTO","COLE_MCPIO_UBICACION","COD_MPIO").show(5, truncate=False)
"""),

    md("## 7. Derivar `ANO` desde PERIODO (YYYYS → YYYY)"),
    code("""
df = df.withColumn("ANO", F.substring(F.col("PERIODO"), 1, 4).cast(IntegerType()))
df.groupBy("ANO").count().orderBy("ANO").show(30)
"""),

    md("## 8. Banding del puntaje global"),
    code("""
df = df.withColumn(
    "RANGO_PUNT_GLOBAL",
    F.when(F.col("PUNT_GLOBAL") < 200, "BAJO")
     .when((F.col("PUNT_GLOBAL") >= 200) & (F.col("PUNT_GLOBAL") < 300), "MEDIO")
     .otherwise("ALTO")
)
df.groupBy("RANGO_PUNT_GLOBAL").count().orderBy(F.desc("count")).show()
"""),

    md("## 9. Selección de columnas finales y persistencia"),
    code("""
# Conservamos columnas útiles para análisis Gold + features ML
COLS_FINAL = [
    "PERIODO", "ANO",
    "ESTU_CONSECUTIVO",
    "COD_DEPTO", "COD_MPIO",
    "COLE_DEPTO_UBICACION", "COLE_MCPIO_UBICACION",
    "COLE_AREA_UBICACION", "COLE_BILINGUE", "COLE_CALENDARIO", "COLE_CARACTER",
    "COLE_NATURALEZA", "COLE_JORNADA", "COLE_GENERO",
    "ESTU_GENERO",
    "FAMI_ESTRATOVIVIENDA",
    "FAMI_TIENEINTERNET", "FAMI_TIENECOMPUTADOR",
    "TIENE_INTERNET_BIN", "TIENE_COMPUTADOR_BIN",
    "PUNT_GLOBAL", "RANGO_PUNT_GLOBAL",
] + [c for c in df.columns if c.startswith("PUNT_") and c not in ("PUNT_GLOBAL",)]

COLS_FINAL = [c for c in COLS_FINAL if c in df.columns]
print("Cols guardadas:", len(COLS_FINAL))

df_silver = df.select(*COLS_FINAL)
df_silver.printSchema()
"""),

    md("## 10. Escribir Silver Parquet (particionado por ANO)"),
    code("""
import time
t0 = time.time()
(df_silver.write
    .mode("overwrite")
    .partitionBy("ANO")
    .option("compression", "snappy")
    .parquet(P.SILVER_ICFES))
print(f"Escrito en {time.time()-t0:.1f}s")
"""),

    md("## 11. Verificación post-escritura"),
    code("""
silver = spark.read.parquet(P.SILVER_ICFES)
n_final = silver.count()
print(f"Filas Silver: {n_final:,}")
print("Distribución por año:")
silver.groupBy("ANO").count().orderBy("ANO").show(30)
print("Promedio de puntaje por año:")
silver.groupBy("ANO").agg(F.round(F.avg("PUNT_GLOBAL"),1).alias("avg_punt"),
                          F.count("*").alias("n")).orderBy("ANO").show(30)
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
