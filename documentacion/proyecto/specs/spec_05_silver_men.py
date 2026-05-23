"""Generates 05_silver_men.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/05_silver_men.ipynb"

cells = [
    md("""
# 05 — Silver: MEN (Estadísticas Educación Municipal)

Indicadores educativos a nivel municipio-año (cobertura, deserción, aprobación, etc.).

## FILTROS APLICADOS

| # | Filtro | Justificación |
|---|---|---|
| F1 | `COD_MPIO IS NOT NULL AND ANO IS NOT NULL` | `(COD_MPIO, ANO)` es la clave compuesta del Silver MEN y la **clave de join con el panel municipal**. Registros sin ambas claves no aportan al pipeline y romperían los joins. |
| F2 | `POBLACION_5_16 > 0` | Municipios sin población infantil reportada (`POBLACION_5_16 ≤ 0` o nulo) corresponden a registros administrativos vacíos del MEN; sus tasas y coberturas son indeterminadas y contaminan los promedios departamentales. |

## TRANSFORMACIONES APLICADAS

| # | Transformación | Justificación |
|---|---|---|
| T1 | **Slugify de columnas**: `unicodedata.normalize('NFD')` + drop combining + `replace(' ','_').upper()` | Los nombres originales tienen acentos (`CÓDIGO`, `POBLACIÓN`, `BÁSICA`) y caracteres especiales (`Ñ`, comas), que rompen `df.select(...)` en Spark. Aplicado a las 41 columnas. |
| T2 | Cast `ANO` → IntegerType ; `lpad(CODIGO_MUNICIPIO, 5)` → `COD_MPIO` ; `lpad(CODIGO_DEPARTAMENTO, 2)` → `COD_DEPTO` | Códigos DANE como string padded son necesarios para join contra ICFES/Internet/SISBEN (donde también están así). |
| T3 | **Parsear porcentajes**: `regexp_replace('%','')` → DoubleType / 100 sobre todas las columnas `TASA_*`, `COBERTURA_*`, `DESERCION*`, `APROBACION*`, `REPROBACION*`, `REPITENCIA*` | Bronze los preserva como strings tipo `"56.11%"`. Sin parsing no se pueden usar como features numéricas en correlaciones, regresión ni K-Means. División por 100 para escala 0..1 (consistente con `pct_internet_icfes`). |
| T4 | Cast `POBLACION_5_16`, `SEDES_CONECTADAS_A_INTERNET` → LongType ; `TAMANO_PROMEDIO_DE_GRUPO` → DoubleType | Enteros y decimales numéricos para agregaciones; sin tipado correcto, `sum/avg` los trata como string. |
| T5 | `partitionBy(ANO)` al escribir Parquet | Acelera queries filtradas por año en Gold y EDA. |
"""),

    md("## 1. Setup y carga"),
    code("""
import sys, unicodedata
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType

spark = get_spark("Entrega2-Silver-MEN", executor_memory="2g", driver_memory="2g", cores=2)
df = spark.read.parquet(P.BRONZE_PQ_MEN)
print(f"Filas: {df.count():,}   Cols: {len(df.columns)}")
print("Columnas originales:")
for c in df.columns: print("  ", repr(c))
"""),

    md("## 2. Slugify de columnas (sin acentos/espacios, UPPER)"),
    code("""
def slugify(name: str) -> str:
    # NFD descompone Á → A + combining acute. Mn = marks, nonspacing.
    no_accent = "".join(ch for ch in unicodedata.normalize("NFD", name) if unicodedata.category(ch) != "Mn")
    return (no_accent.replace(" ", "_")
                     .replace("-", "_")
                     .replace(",", "")
                     .upper())

rename_map = {c: slugify(c) for c in df.columns}
df_r = df
for old, new in rename_map.items():
    df_r = df_r.withColumnRenamed(old, new)
print("Renombres aplicados:", sum(1 for k,v in rename_map.items() if k != v))
print("Columnas nuevas (primeras 15):", df_r.columns[:15])
"""),

    md("## 3. Tipificar año y códigos DANE"),
    code("""
df_r = (df_r
    .withColumn("ANO", F.col("ANO").cast(IntegerType()))
    .withColumn("CODIGO_MUNICIPIO",    F.lpad(F.col("CODIGO_MUNICIPIO"), 5, "0"))
    .withColumn("CODIGO_DEPARTAMENTO", F.lpad(F.col("CODIGO_DEPARTAMENTO"), 2, "0"))
)
df_r = df_r.withColumnRenamed("CODIGO_MUNICIPIO", "COD_MPIO")
df_r = df_r.withColumnRenamed("CODIGO_DEPARTAMENTO", "COD_DEPTO")
df_r.select("ANO","COD_MPIO","MUNICIPIO","COD_DEPTO","DEPARTAMENTO","POBLACION_5_16").show(5, truncate=False)
"""),

    md("## 3b. Aplicación de filtros (F1 + F2)"),
    code("""
n0 = df_r.count()
print(f"Filas antes de filtros: {n0:,}")

# F1 — COD_MPIO y ANO no nulos (claves de join con el panel)
df_r = df_r.filter(F.col("COD_MPIO").isNotNull() & F.col("ANO").isNotNull())
n1 = df_r.count()
print(f"  Tras F1 (COD_MPIO y ANO no nulos)        : {n1:>8,}  (eliminó {n0-n1:,})")

# F2 — POBLACION_5_16 > 0 (municipios con población infantil reportada)
df_r = df_r.filter(F.col("POBLACION_5_16").cast(LongType()) > 0)
n2 = df_r.count()
print(f"  Tras F2 (POBLACION_5_16 > 0)             : {n2:>8,}  (eliminó {n1-n2:,})")
print(f"Total eliminado por filtros: {n0-n2:,} ({100*(n0-n2)/n0:.2f}%)")
"""),

    md("## 4. Parsear porcentajes  '56.11%' → 0.5611"),
    code("""
# Toda columna que contenga '%' en cualquier fila la tratamos como porcentaje
def parse_pct(col):
    cleaned = F.regexp_replace(col, "%", "")
    cleaned = F.when(cleaned == "", None).otherwise(cleaned)
    cleaned = F.when(cleaned == "NULL", None).otherwise(cleaned)
    return (cleaned.cast(DoubleType()) / 100.0)

# Columnas que sabemos que son porcentaje (por el muestreo)
pct_cols = [c for c in df_r.columns if c.startswith("TASA_") or c.startswith("COBERTURA_")
            or c.startswith("DESERCION") or c.startswith("APROBACION")
            or c.startswith("REPROBACION") or c.startswith("REPITENCIA")]
print(f"Columnas porcentaje detectadas: {len(pct_cols)}")
for c in pct_cols:
    df_r = df_r.withColumn(c, parse_pct(F.col(c)))

# POBLACION y TAMAÑO_PROMEDIO_DE_GRUPO y SEDES_CONECTADAS_A_INTERNET son numéricos enteros
for c in ("POBLACION_5_16", "SEDES_CONECTADAS_A_INTERNET"):
    if c in df_r.columns:
        df_r = df_r.withColumn(c, F.col(c).cast(LongType()))
if "TAMANO_PROMEDIO_DE_GRUPO" in df_r.columns:
    df_r = df_r.withColumn("TAMANO_PROMEDIO_DE_GRUPO", F.col("TAMANO_PROMEDIO_DE_GRUPO").cast(DoubleType()))

df_r.printSchema()
df_r.select("ANO","COD_MPIO","COBERTURA_NETA","DESERCION","APROBACION","POBLACION_5_16").show(5)
"""),

    md("## 5. Escribir Silver Parquet"),
    code("""
import time
t0 = time.time()
(df_r.write
    .mode("overwrite")
    .partitionBy("ANO")
    .option("compression","snappy")
    .parquet(P.SILVER_MEN))
print(f"Escrito en {time.time()-t0:.1f}s")

sv = spark.read.parquet(P.SILVER_MEN)
print(f"Verificación: {sv.count():,} filas")
sv.groupBy("ANO").count().orderBy("ANO").show()
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
