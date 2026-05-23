"""Generates 04_silver_sisben.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/04_silver_sisben.ipynb"

cells = [
    md("""
# 04 — Silver: SISBEN (DNP) — agregado a nivel municipio

El SISBEN viene a nivel persona (4.5M filas). Para joins con ICFES / Internet / MEN
necesitamos **agregar a nivel municipio**.

## FILTROS APLICADOS (a nivel persona, antes de agregar)

| # | Filtro | Justificación |
|---|---|---|
| F1 | `cod_mpio IS NOT NULL` | El código DANE de municipio es la **clave de agregación** y la **clave de join** con todas las demás tablas. Una persona sin `cod_mpio` no puede ser contabilizada en su municipio y rompería los joins downstream. |
| F2 | `Grupo IN ('A','B','C','D')` | El SISBEN clasifica oficialmente a las personas en estos 4 grupos. Cualquier valor distinto (`NULL`, `''`, `Z`, etc.) es **error de captura** y contaminaría las métricas `pct_grupo_*`. |

## TRANSFORMACIONES APLICADAS

| # | Transformación | Justificación |
|---|---|---|
| T1 | Cast `I1..I15` (15 indicadores) → IntegerType | Bronze los preserva como string; necesitamos numéricos para calcular `avg(I_k)` y el índice de privación. |
| T2 | `lpad(cod_mpio, 5, '0')` → `COD_MPIO` (string) | Códigos DANE son siempre 5 dígitos; mantener como string evita perder el cero inicial (ej. `5001` → `05001`). |
| T3 | `Grupo → UPPER` ; cast `ZONA` → IntegerType ; cast `FEX` → DoubleType | Estandarización de tipos para agregaciones consistentes. |
| T4 | **Agregación masiva** persona → municipio: `groupBy(COD_MPIO).agg(...)` con n_personas, n_hogares, fex_total, pct_rural, pct_grupo_A/B/C/D, avg_I1..I15 | Reduce 4.5M personas → ~1100 municipios, grano necesario para join con ICFES/Internet/MEN. Las métricas de grupo y ruralidad se calculan como promedio de indicador booleano (técnica estándar para % a partir de microdatos). |
| T5 | Derivación `idx_privacion = avg_I1 + ... + avg_I15` | **Proxy de pobreza multidimensional** a nivel municipio (rango 0..15). Variable derivada clave para el ML. |

**Variables clave de entrada:**
- `cod_mpio`: código DANE.
- `I1..I15`: 15 indicadores binarios de privación (1 = privado).
- `Grupo` (A=más pobre, D=menos), `Nivel`, `Clasificacion` (e.g. A1, B2).
- `ZONA` (1=urbana, 2=rural).
- `FEX`: factor de expansión poblacional.
"""),

    md("## 1. Setup y carga"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

spark = get_spark("Entrega2-Silver-SISBEN", executor_memory="4g", driver_memory="2g", cores=2)
df = spark.read.parquet(P.BRONZE_PQ_SISBEN)
print(f"Filas persona: {df.count():,}   Cols: {len(df.columns)}")
print("Cols clave :", [c for c in df.columns if c in ("cod_mpio","H_5","Grupo","Nivel","Clasificacion","ZONA","HOGAR","FEX")])
df.select("cod_mpio","Grupo","Nivel","Clasificacion","ZONA","HOGAR","FEX","I1","I2","I15").show(5)
"""),

    md("## 2. Tipificar y normalizar (T1, T2, T3)"),
    code("""
indicadores = [f"I{i}" for i in range(1, 16)]
df_n = df
for c in indicadores:
    df_n = df_n.withColumn(c, F.col(c).cast(IntegerType()))
df_n = (df_n
    .withColumn("COD_MPIO", F.lpad(F.col("cod_mpio"), 5, "0"))
    .withColumn("FEX",      F.col("FEX").cast(DoubleType()))
    .withColumn("ZONA",     F.col("ZONA").cast(IntegerType()))
    .withColumn("Grupo",    F.upper(F.col("Grupo")))
)
df_n.select("COD_MPIO","Grupo","ZONA","FEX","I1","I15").show(5)
"""),

    md("## 3. Aplicación de filtros (F1 + F2)"),
    code("""
n0 = df_n.count()
print(f"Personas antes de filtros: {n0:,}")

# F1 — cod_mpio no nulo (es la clave de agregación y de join)
df_n = df_n.filter(F.col("COD_MPIO").isNotNull() & (F.col("COD_MPIO") != ""))
n1 = df_n.count()
print(f"  Tras F1 (COD_MPIO no nulo)             : {n1:>10,}  (eliminó {n0-n1:,})")

# F2 — Grupo en valores válidos del SISBEN
df_n = df_n.filter(F.col("Grupo").isin("A","B","C","D"))
n2 = df_n.count()
print(f"  Tras F2 (Grupo IN A/B/C/D)             : {n2:>10,}  (eliminó {n1-n2:,})")
print(f"Total eliminado por filtros: {n0-n2:,} ({100*(n0-n2)/n0:.2f}%)")
"""),

    md("## 4. Agregación por municipio (T4 + T5)"),
    code("""
agg_exprs = [
    F.count("*").alias("n_personas"),
    F.countDistinct(F.concat_ws("|", F.col("CORTE"), F.col("COD_MPIO"), F.col("HOGAR"))).alias("n_hogares"),
    F.sum("FEX").alias("fex_total"),
    F.avg(F.when(F.col("ZONA")==2, 1.0).otherwise(0.0)).alias("pct_rural"),
    F.avg(F.when(F.col("Grupo")=="A", 1.0).otherwise(0.0)).alias("pct_grupo_A"),
    F.avg(F.when(F.col("Grupo")=="B", 1.0).otherwise(0.0)).alias("pct_grupo_B"),
    F.avg(F.when(F.col("Grupo")=="C", 1.0).otherwise(0.0)).alias("pct_grupo_C"),
    F.avg(F.when(F.col("Grupo")=="D", 1.0).otherwise(0.0)).alias("pct_grupo_D"),
]
for i in indicadores:
    agg_exprs.append(F.avg(F.col(i)).alias(f"avg_{i}"))

sisben_mpio = (
    df_n.groupBy("COD_MPIO")
    .agg(*agg_exprs)
)

# Índice de privación: suma promedio de los 15 indicadores → 0..15
priv_cols = [F.col(f"avg_I{i}") for i in range(1, 16)]
sisben_mpio = sisben_mpio.withColumn("idx_privacion", sum(priv_cols))

print(f"Municipios distintos: {sisben_mpio.count():,}")
sisben_mpio.select("COD_MPIO","n_personas","n_hogares","pct_rural","pct_grupo_A","idx_privacion").show(10)
"""),

    md("## 5. Estadísticos del índice de privación"),
    code("""
sisben_mpio.select("idx_privacion","pct_grupo_A","pct_rural").describe().show()
"""),

    md("## 6. Escribir Silver Parquet"),
    code("""
import time
t0 = time.time()
(sisben_mpio.write
    .mode("overwrite")
    .option("compression","snappy")
    .parquet(P.SILVER_SISBEN_MPIO))
print(f"Escrito en {time.time()-t0:.1f}s")

sv = spark.read.parquet(P.SILVER_SISBEN_MPIO)
print(f"Verificación: {sv.count():,} filas (1 por municipio)")
sv.orderBy(F.desc("idx_privacion")).show(10)
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
