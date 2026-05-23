"""Generates 06_gold_panel_municipal.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/06_gold_panel_municipal.ipynb"

cells = [
    md("""
# 06 — Gold: Panel municipal año (mesa analítica final)

Cruza las 4 fuentes Silver en una sola **tabla de panel** con grano
`(COD_MPIO, ANO)`. Esta es la mesa que alimenta EDA y modelos ML.

**Plan de joins:**
1. **ICFES → municipal-año**: avg(PUNT_GLOBAL), avg(PUNT_*), n_estudiantes,
   pct_internet, pct_computador, pct_rural, pct_oficial.
2. **Internet → municipal-año**: total_accesos, avg_velocidad_bajada, n_proveedores.
3. **MEN → municipal-año**: cobertura_neta, deserción, aprobación, sedes_conectadas.
4. **SISBEN → municipal (sin año)**: idx_privacion, pct_grupo_A, pct_rural, n_personas.

Join principal: `LEFT JOIN` partiendo de ICFES (que es el outcome de interés).
SISBEN se broadcasta porque es chico (~1100 filas, una por municipio).
"""),

    md("## 1. Setup y carga de Silver"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = get_spark("Entrega2-Gold-Panel", executor_memory="4g", driver_memory="2g", cores=2)

icfes    = spark.read.parquet(P.SILVER_ICFES)
internet = spark.read.parquet(P.SILVER_INTERNET)
sisben   = spark.read.parquet(P.SILVER_SISBEN_MPIO)
men      = spark.read.parquet(P.SILVER_MEN)

for name, df in [("icfes",icfes),("internet",internet),("sisben",sisben),("men",men)]:
    print(f"{name:>8}: {df.count():>10,} filas   {len(df.columns)} cols")
"""),

    md("## 2. Agregar ICFES a (COD_MPIO, ANO)"),
    code("""
icfes_agg = (icfes
    .groupBy("COD_MPIO", "ANO", "COLE_DEPTO_UBICACION")
    .agg(
        F.count("*").alias("n_estudiantes"),
        F.round(F.avg("PUNT_GLOBAL"), 2).alias("avg_punt_global"),
        F.round(F.stddev("PUNT_GLOBAL"), 2).alias("sd_punt_global"),
        F.round(F.percentile_approx("PUNT_GLOBAL", 0.5), 1).alias("med_punt_global"),
        F.round(F.avg("PUNT_LECTURA_CRITICA"), 2).alias("avg_punt_lectura"),
        F.round(F.avg("PUNT_MATEMATICAS"), 2).alias("avg_punt_matematicas"),
        F.round(F.avg("PUNT_C_NATURALES"), 2).alias("avg_punt_naturales"),
        F.round(F.avg("PUNT_SOCIALES_CIUDADANAS"), 2).alias("avg_punt_sociales"),
        F.round(F.avg("PUNT_INGLES"), 2).alias("avg_punt_ingles"),
        F.round(F.avg("TIENE_INTERNET_BIN"), 4).alias("pct_internet_icfes"),
        F.round(F.avg("TIENE_COMPUTADOR_BIN"), 4).alias("pct_computador_icfes"),
        F.round(F.avg(F.when(F.col("COLE_AREA_UBICACION")=="RURAL", 1.0).otherwise(0.0)), 4).alias("pct_rural_colegio"),
        F.round(F.avg(F.when(F.col("COLE_NATURALEZA")=="OFICIAL", 1.0).otherwise(0.0)), 4).alias("pct_colegio_oficial"),
    )
)
print(f"ICFES agg rows (mpio-año): {icfes_agg.count():,}")
icfes_agg.orderBy(F.desc("avg_punt_global")).show(5, truncate=False)
"""),

    md("## 3. Agregar Internet Fijo a (COD_MPIO, ANO)"),
    code("""
internet_agg = (internet
    .groupBy("COD_MPIO", "ANO")
    .agg(
        F.sum("NUM_ACCESOS").alias("total_accesos"),
        F.countDistinct("PROVEEDOR").alias("n_proveedores"),
        F.round(F.avg("VELOCIDAD_BAJADA"), 2).alias("avg_velocidad_bajada"),
        F.round(F.avg("VELOCIDAD_SUBIDA"), 2).alias("avg_velocidad_subida"),
        F.countDistinct("TECNOLOGIA").alias("n_tecnologias"),
        F.sum(F.when(F.col("SEGMENTO")=="RESIDENCIAL", F.col("NUM_ACCESOS")).otherwise(0)).alias("accesos_residenciales"),
        F.sum(F.when(F.col("SEGMENTO")=="CORPORATIVO", F.col("NUM_ACCESOS")).otherwise(0)).alias("accesos_corporativos"),
    )
)
print(f"Internet agg rows (mpio-año): {internet_agg.count():,}")
internet_agg.orderBy(F.desc("total_accesos")).show(5)
"""),

    md("## 4. MEN ya está a nivel municipio-año"),
    code("""
men_sel = men.select(
    "COD_MPIO", "ANO",
    "POBLACION_5_16", "TASA_MATRICULACION_5_16",
    "COBERTURA_NETA", "COBERTURA_NETA_PRIMARIA", "COBERTURA_NETA_SECUNDARIA", "COBERTURA_NETA_MEDIA",
    "DESERCION", "APROBACION", "REPROBACION", "REPITENCIA",
    "TAMANO_PROMEDIO_DE_GRUPO", "SEDES_CONECTADAS_A_INTERNET",
).filter(F.col("COD_MPIO").isNotNull())
print(f"MEN rows (mpio-año): {men_sel.count():,}")
"""),

    md("## 5. SISBEN es por municipio (sin año)"),
    code("""
sisben_sel = sisben.select(
    "COD_MPIO",
    F.col("n_personas").alias("sisben_n_personas"),
    F.col("n_hogares").alias("sisben_n_hogares"),
    F.col("fex_total").alias("sisben_poblacion_expandida"),
    "idx_privacion",
    "pct_grupo_A", "pct_grupo_B", "pct_grupo_C", "pct_grupo_D",
    F.col("pct_rural").alias("pct_rural_sisben"),
)
print(f"SISBEN rows (mpio): {sisben_sel.count():,}")
"""),

    md("## 6. JOIN: panel municipal año"),
    code("""
panel = (
    icfes_agg
    .join(internet_agg, on=["COD_MPIO","ANO"], how="left")
    .join(men_sel,      on=["COD_MPIO","ANO"], how="left")
    .join(broadcast(sisben_sel), on="COD_MPIO", how="left")
)
print(f"PANEL rows: {panel.count():,}")
print(f"PANEL cols: {len(panel.columns)}")
panel.printSchema()
"""),

    md("## 7. Variables derivadas: accesos per cápita, etc."),
    code("""
panel = (panel
    .withColumn("accesos_per_capita_5_16",
                F.when(F.col("POBLACION_5_16") > 0,
                       F.col("total_accesos") / F.col("POBLACION_5_16")))
    .withColumn("brecha_internet_icfes_vs_real",
                F.col("pct_internet_icfes") - F.col("accesos_per_capita_5_16"))
)
"""),

    md("## 8. Escribir Gold Parquet (particionado por ANO)"),
    code("""
import time
t0 = time.time()
(panel.write
    .mode("overwrite")
    .partitionBy("ANO")
    .option("compression","snappy")
    .parquet(P.GOLD_PANEL_MUNICIPAL))
print(f"Escrito en {time.time()-t0:.1f}s")
"""),

    md("## 9. Verificación + vista preliminar"),
    code("""
g = spark.read.parquet(P.GOLD_PANEL_MUNICIPAL)
print(f"Gold rows: {g.count():,}")
print("\\nTop municipios por puntaje global promedio (año más reciente):")
ano_max = g.agg(F.max("ANO")).first()[0]
print("Año más reciente:", ano_max)
g.filter(F.col("ANO") == ano_max) \\
 .select("COD_MPIO","COLE_DEPTO_UBICACION","n_estudiantes","avg_punt_global",
         "pct_internet_icfes","idx_privacion","total_accesos") \\
 .orderBy(F.desc("avg_punt_global")) \\
 .show(15, truncate=False)
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
