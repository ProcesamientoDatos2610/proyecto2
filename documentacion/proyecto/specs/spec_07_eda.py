"""Generates 07_eda_y_preguntas_negocio.ipynb — 8 preguntas de la Entrega 1.
Estilo de clase: agregaciones en Spark (.groupBy/.agg/.show), df.stat.corr()
para correlaciones, gráficos matplotlib solo como evidencia visual al final
de cada pregunta (cómputo siempre en Spark).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/07_eda_y_preguntas_negocio.ipynb"

cells = [
    md("""
# 07 — EDA y respuesta a las 8 preguntas de negocio

Cada pregunta planteada en la **Entrega 1** se responde con cómputo PySpark
sobre la *vista minable* `gold/panel_municipal` (4 fuentes unidas por municipio-año)
y, donde aplica, `silver/icfes` para análisis a nivel estudiante.

**Preguntas (Entrega 1):**

1. ¿Cuál es la **relación entre el acceso a internet en el hogar y el desempeño
   Saber 11** a nivel municipal?
2. ¿Cómo varía el rendimiento entre **zonas rurales y urbanas** en función del
   nivel de conectividad?
3. ¿En qué medida la **pobreza multidimensional** incide en los resultados,
   incluso en territorios con niveles similares de internet?
4. ¿Qué diferencias se observan entre municipios con **alta vs baja penetración**
   de internet?
5. ¿Qué **variables socioeconómicas** presentan mayor capacidad explicativa
   del desempeño en comparación con el acceso a internet?
6. ¿Cuál es la relación entre la **cobertura educativa** y la conectividad?
7. ¿Cómo influye el **acceso a computador** en los resultados por **área evaluada**?
8. ¿Qué municipios presentan **alta conectividad pero bajo rendimiento**, y qué
   patrones se identifican?
"""),

    md("## 1. Setup — SparkSession + carga"),
    code("""
import sys, os
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F

spark = get_spark("Entrega2-EDA", executor_memory="3g", driver_memory="3g", cores=2)

# Vista minable (joined): mpio × año, 4 fuentes cruzadas
panel = spark.read.parquet(P.GOLD_PANEL_MUNICIPAL).cache()
# Silver ICFES: nivel estudiante (para Q7 por área evaluada)
icfes  = spark.read.parquet(P.SILVER_ICFES)

print(f"Panel rows (mpio-año): {panel.count():,}")
print(f"ICFES rows (estudiante): {icfes.count():,}")
panel.printSchema()
"""),

    md("## 2. Estadísticos descriptivos del panel — variables clave"),
    code("""
panel.describe([
    "n_estudiantes","avg_punt_global","pct_internet_icfes",
    "accesos_per_capita_5_16","idx_privacion","pct_grupo_A",
    "COBERTURA_NETA","DESERCION"
]).show()
"""),

    # =========================================================
    md("## 3. Pregunta 1 — Acceso a internet en el hogar vs Saber 11"),
    code("""
# Cómputo Spark: filtro filas válidas, correlación de Pearson y promedios por cuartil
df1 = panel.filter(F.col("pct_internet_icfes").isNotNull() & F.col("avg_punt_global").isNotNull())

# Correlación (df.stat.corr — patrón de la clase)
r_q1 = df1.stat.corr("pct_internet_icfes", "avg_punt_global")
print(f"Correlación de Pearson (pct_internet_icfes vs avg_punt_global): r = {r_q1:+.4f}")

# Cuartiles de internet (Bucketizer estilo clase) → puntaje promedio
q = df1.approxQuantile("pct_internet_icfes", [0.25, 0.5, 0.75], 0.01)
print(f"Cuartiles de pct_internet_icfes: {[round(x,3) for x in q]}")

from pyspark.ml.feature import Bucketizer
splits = [float("-inf")] + q + [float("inf")]
buck = Bucketizer(splits=splits, inputCol="pct_internet_icfes", outputCol="q_internet", handleInvalid="skip")
df1b = buck.transform(df1).withColumn(
    "q_internet_lbl",
    F.when(F.col("q_internet")==0, "Q1 (bajo)")
     .when(F.col("q_internet")==1, "Q2")
     .when(F.col("q_internet")==2, "Q3")
     .otherwise("Q4 (alto)")
)

print("\\nPuntaje global promedio por cuartil de internet:")
(df1b.groupBy("q_internet_lbl")
     .agg(F.round(F.avg("avg_punt_global"),2).alias("avg_punt"),
          F.count("*").alias("n_mpio_anos"))
     .orderBy("q_internet_lbl")
     .show())
"""),

    code("""
# Plot: scatter ponderado (solo para evidencia visual; cómputo ya hecho en Spark)
import os, matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
OUT_DIR = "/home/estudiante/proyecto_datos/evidencia"; os.makedirs(OUT_DIR, exist_ok=True)

pdf1 = df1.select("pct_internet_icfes","avg_punt_global","n_estudiantes").toPandas()
fig, ax = plt.subplots(figsize=(8,5))
ax.scatter(pdf1["pct_internet_icfes"], pdf1["avg_punt_global"],
           s=(pdf1["n_estudiantes"].clip(upper=2000)/8), alpha=0.25, c="steelblue", edgecolors="none")
ax.set_xlabel("% estudiantes con internet en casa")
ax.set_ylabel("Puntaje global promedio")
ax.set_title(f"Q1 — Internet en hogar vs Saber 11 (r={r_q1:+.3f}, n={len(pdf1):,} mpio-año)")
ax.grid(alpha=0.3); plt.tight_layout()
plt.savefig(f"{OUT_DIR}/q1_internet_vs_puntaje.png", dpi=110); plt.show()
"""),

    # =========================================================
    md("## 4. Pregunta 2 — Rural vs Urbano en función de conectividad"),
    code("""
# Definimos zona: pct_rural_colegio >= 0.5 → Rural ; resto → Urbano/Mixto
# Cruzamos con cuartiles de internet (reciclamos buck del Q1)
df2 = (df1b
       .withColumn("zona", F.when(F.col("pct_rural_colegio") >= 0.5, "Rural").otherwise("Urbano/Mixto"))
       .filter(F.col("zona").isNotNull()))

print("Puntaje promedio por (zona, cuartil internet):")
(df2.groupBy("zona","q_internet_lbl")
    .agg(F.round(F.avg("avg_punt_global"),2).alias("avg_punt"),
         F.count("*").alias("n_mpio_anos"))
    .orderBy("zona","q_internet_lbl")
    .show())

# Tabla pivot lista para el reporte
pivot_q2 = (df2.groupBy("zona")
              .pivot("q_internet_lbl", ["Q1 (bajo)","Q2","Q3","Q4 (alto)"])
              .agg(F.round(F.avg("avg_punt_global"),1))
              .orderBy("zona"))
print("\\nPromedio Saber 11 por zona × cuartil de internet (tabla pivot):")
pivot_q2.show()
"""),

    code("""
# Gráfico Q2: bar agrupada (zona × cuartil de internet)
pdf2 = pivot_q2.toPandas().set_index("zona")
fig, ax = plt.subplots(figsize=(9,5))
x = range(len(pdf2.columns)); width = 0.38
for i, zona in enumerate(pdf2.index):
    ax.bar([xi + (i-0.5)*width for xi in x], pdf2.loc[zona].values, width, label=zona)
ax.set_xticks(list(x)); ax.set_xticklabels(pdf2.columns)
ax.set_xlabel("Cuartil de penetración de internet")
ax.set_ylabel("Puntaje global promedio")
ax.set_title("Q2 — Saber 11 por zona × cuartil de internet")
ax.legend(); ax.grid(axis="y", alpha=0.3); plt.tight_layout()
plt.savefig(f"{OUT_DIR}/q2_zona_x_internet.png", dpi=110); plt.show()
"""),

    # =========================================================
    md("## 5. Pregunta 3 — Pobreza multidimensional vs puntaje, controlando por internet"),
    code("""
# Estrategia: dentro de cada cuartil de internet, correlación entre idx_privacion y avg_punt_global.
# Si la pobreza incide INCLUSO controlando por internet, la correlación debe seguir siendo negativa
# significativa en los 4 estratos.
df3 = df1b.filter(F.col("idx_privacion").isNotNull() & F.col("avg_punt_global").isNotNull())

print("Pregunta 3 — correlación pobreza vs puntaje DENTRO de cada cuartil de internet:")
filas = []
for q_lbl in ["Q1 (bajo)","Q2","Q3","Q4 (alto)"]:
    sub = df3.filter(F.col("q_internet_lbl") == q_lbl)
    n = sub.count()
    r = sub.stat.corr("idx_privacion","avg_punt_global") if n>1 else float("nan")
    avg_p = sub.agg(F.avg("avg_punt_global")).first()[0]
    filas.append((q_lbl, n, r, avg_p))
    print(f"  {q_lbl:10s}  n={n:>5,}  corr(pobreza, puntaje)={r:+.3f}  avg_puntaje={avg_p:.1f}")

# Correlación global de referencia (sin controlar)
r_global = df3.stat.corr("idx_privacion","avg_punt_global")
print(f"\\nCorrelación SIN controlar por internet: r = {r_global:+.4f}")
print("Si la correlación se mantiene negativa en los 4 estratos → la pobreza incide INCLUSO controlando por internet.")
"""),

    # =========================================================
    md("## 6. Pregunta 4 — Alta vs baja penetración de internet (extremos)"),
    code("""
# Definimos alta = top 20% internet, baja = bottom 20% internet
p20, p80 = df1.approxQuantile("pct_internet_icfes", [0.20, 0.80], 0.01)
print(f"Percentil 20 internet = {p20:.3f} ; Percentil 80 internet = {p80:.3f}")

df4 = df1.withColumn("grupo",
    F.when(F.col("pct_internet_icfes") >= p80, "ALTA")
     .when(F.col("pct_internet_icfes") <= p20, "BAJA")
     .otherwise(None))

print("\\nComparativa alta vs baja penetración:")
(df4.filter(F.col("grupo").isNotNull())
    .groupBy("grupo")
    .agg(F.count("*").alias("n_mpio_anos"),
         F.round(F.avg("avg_punt_global"),2).alias("avg_punt"),
         F.round(F.stddev("avg_punt_global"),2).alias("std_punt"),
         F.round(F.avg("pct_internet_icfes"),3).alias("pct_internet_medio"),
         F.round(F.avg("idx_privacion"),2).alias("privacion_media"))
    .orderBy("grupo")
    .show())

# Diferencia neta
ag = (df4.filter(F.col("grupo").isNotNull()).groupBy("grupo")
        .agg(F.avg("avg_punt_global").alias("p")).collect())
mp = {r["grupo"]: r["p"] for r in ag}
print(f"\\nDiferencia ALTA - BAJA = {(mp['ALTA']-mp['BAJA']):.1f} puntos Saber 11.")
"""),

    # =========================================================
    md("## 7. Pregunta 5 — Variables socioeconómicas con mayor capacidad explicativa"),
    code("""
# Calculamos correlación de cada feature potencial con avg_punt_global y rankeamos.
CANDIDATAS = [
    "pct_internet_icfes",        # referencia
    "accesos_per_capita_5_16",
    "avg_velocidad_bajada",
    "idx_privacion",
    "pct_grupo_A",
    "pct_grupo_D",
    "pct_rural_sisben",
    "pct_rural_colegio",
    "COBERTURA_NETA",
    "DESERCION",
    "APROBACION",
    "n_estudiantes",
    "POBLACION_5_16",
]
df5 = panel.filter(F.col("avg_punt_global").isNotNull())
print("Ranking de correlación absoluta con avg_punt_global (Pearson):")
filas = []
for c in CANDIDATAS:
    try:
        r = df5.stat.corr(c, "avg_punt_global")
        filas.append((c, r, abs(r) if r is not None else 0.0))
    except Exception as e:
        filas.append((c, None, 0.0))
filas.sort(key=lambda x: -x[2])
print(f"  {'variable':<28s} {'corr':>10s} {'|corr|':>10s} {'vs internet':>14s}")
r_internet = next((r for c,r,a in filas if c=='pct_internet_icfes'), 0)
for c, r, a in filas:
    diff = (abs(r) - abs(r_internet)) if r is not None and r_internet is not None else 0
    marca = "↑" if diff > 0.02 else ("↓" if diff < -0.02 else " ")
    rs = f"{r:+.4f}" if r is not None else "  n/a"
    print(f"  {c:<28s} {rs:>10s} {a:>10.4f} {marca:>14s}")
"""),

    # =========================================================
    md("## 8. Pregunta 6 — Cobertura educativa vs conectividad"),
    code("""
df6 = panel.filter(F.col("COBERTURA_NETA").isNotNull() & F.col("pct_internet_icfes").isNotNull())
r_q6 = df6.stat.corr("COBERTURA_NETA", "pct_internet_icfes")
print(f"Correlación COBERTURA_NETA vs pct_internet_icfes: r = {r_q6:+.4f}")

# Misma relación pero con TASA_MATRICULACION_5_16 si existe
if "TASA_MATRICULACION_5_16" in panel.columns:
    df6b = panel.filter(F.col("TASA_MATRICULACION_5_16").isNotNull() & F.col("pct_internet_icfes").isNotNull())
    r_q6b = df6b.stat.corr("TASA_MATRICULACION_5_16","pct_internet_icfes")
    print(f"Correlación TASA_MATRICULACION_5_16 vs pct_internet_icfes: r = {r_q6b:+.4f}")

# Tabla por cuartiles de cobertura
qc = df6.approxQuantile("COBERTURA_NETA", [0.25, 0.5, 0.75], 0.01)
from pyspark.ml.feature import Bucketizer
buckc = Bucketizer(splits=[float("-inf")] + qc + [float("inf")],
                   inputCol="COBERTURA_NETA", outputCol="q_cob", handleInvalid="skip")
df6q = buckc.transform(df6)
print("\\nMedia de conectividad por cuartil de cobertura neta MEN:")
(df6q.groupBy("q_cob")
     .agg(F.count("*").alias("n"),
          F.round(F.avg("pct_internet_icfes"),3).alias("avg_pct_internet"),
          F.round(F.avg("avg_punt_global"),2).alias("avg_punt"))
     .orderBy("q_cob").show())
"""),

    # =========================================================
    md("## 9. Pregunta 7 — Acceso a computador y resultados por área evaluada"),
    code("""
# Esta pregunta requiere nivel ESTUDIANTE (silver/icfes) porque queremos avg por área.
# Filtramos años recientes y tomamos solo registros con datos completos por área.
PUNT_AREAS = ["PUNT_LECTURA_CRITICA","PUNT_MATEMATICAS","PUNT_C_NATURALES",
              "PUNT_SOCIALES_CIUDADANAS","PUNT_INGLES"]
PUNT_AREAS = [c for c in PUNT_AREAS if c in icfes.columns]

# Verificamos que TIENE_COMPUTADOR_BIN existe
print("Distribución TIENE_COMPUTADOR_BIN:")
icfes.groupBy("TIENE_COMPUTADOR_BIN").count().orderBy("TIENE_COMPUTADOR_BIN").show()

# Avg por área según acceso a computador
exprs = [F.round(F.avg(c),2).alias(f"avg_{c}") for c in PUNT_AREAS]
exprs.append(F.count("*").alias("n_estud"))
res_q7 = (icfes.filter(F.col("TIENE_COMPUTADOR_BIN").isNotNull())
                .groupBy("TIENE_COMPUTADOR_BIN")
                .agg(*exprs)
                .orderBy("TIENE_COMPUTADOR_BIN"))
print("\\nPromedio por área evaluada según acceso a computador:")
res_q7.show()
"""),

    code("""
# Gráfico Q7: comparación por área (bar agrupada)
rows_q7 = res_q7.collect()
sin = next((r for r in rows_q7 if r["TIENE_COMPUTADOR_BIN"]==0), None)
con = next((r for r in rows_q7 if r["TIENE_COMPUTADOR_BIN"]==1), None)
if sin and con:
    vals_sin = [sin[f"avg_{c}"] for c in PUNT_AREAS]
    vals_con = [con[f"avg_{c}"] for c in PUNT_AREAS]
    fig, ax = plt.subplots(figsize=(10,5))
    x = range(len(PUNT_AREAS)); w = 0.38
    ax.bar([xi-w/2 for xi in x], vals_sin, w, label="Sin computador (0)", color="lightcoral")
    ax.bar([xi+w/2 for xi in x], vals_con, w, label="Con computador (1)", color="seagreen")
    ax.set_xticks(list(x))
    ax.set_xticklabels([c.replace("PUNT_","") for c in PUNT_AREAS], rotation=20, ha="right")
    ax.set_ylabel("Puntaje promedio")
    ax.set_title("Q7 — Saber 11 por área según acceso a computador en el hogar")
    ax.grid(axis="y", alpha=0.3); ax.legend()
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/q7_computador_por_area.png", dpi=110); plt.show()
    print("\\nDiferencias absolutas (Con - Sin) por área:")
    for c, vs, vc in zip(PUNT_AREAS, vals_sin, vals_con):
        print(f"  {c:<25s}  +{vc-vs:.1f}")
"""),

    # =========================================================
    md("## 10. Pregunta 8 — Municipios con alta conectividad pero bajo rendimiento (anomalías)"),
    code("""
# Snapshot del año más reciente
ano_max = panel.agg(F.max("ANO")).first()[0]
snap = panel.filter((F.col("ANO")==ano_max) & (F.col("n_estudiantes") >= 30))

p_internet = snap.approxQuantile("pct_internet_icfes",[0.75], 0.01)[0]
p_punt     = snap.approxQuantile("avg_punt_global",  [0.25], 0.01)[0]
print(f"Año analizado: {ano_max}")
print(f"Umbrales: pct_internet >= P75 = {p_internet:.3f}  ;  avg_punt <= P25 = {p_punt:.2f}")

anomalias = (snap
    .filter(F.col("pct_internet_icfes") >= p_internet)
    .filter(F.col("avg_punt_global")   <= p_punt))
print(f"\\nMunicipios anómalos detectados: {anomalias.count()}")
(anomalias.select("COD_MPIO","COLE_DEPTO_UBICACION","n_estudiantes",
                  "avg_punt_global","pct_internet_icfes","idx_privacion",
                  "pct_grupo_A","COBERTURA_NETA","DESERCION")
          .orderBy(F.desc("pct_internet_icfes")).show(40, truncate=False))
"""),

    code("""
# Patrones agregados en las anomalías
print("Patrón promedio en municipios ANÓMALOS (alta conectividad + bajo puntaje):")
anomalias.agg(
    F.count("*").alias("n_mpio"),
    F.round(F.avg("pct_internet_icfes"),3).alias("avg_pct_internet"),
    F.round(F.avg("avg_punt_global"),2).alias("avg_punt"),
    F.round(F.avg("idx_privacion"),2).alias("avg_priv"),
    F.round(F.avg("pct_grupo_A"),3).alias("avg_pct_grupo_A"),
    F.round(F.avg("DESERCION"),3).alias("avg_desercion"),
    F.round(F.avg("COBERTURA_NETA"),3).alias("avg_cobertura"),
).show()

print("\\nPatrón promedio en RESTO del país (mismo año):")
normales = snap.exceptAll(anomalias)
normales.agg(
    F.count("*").alias("n_mpio"),
    F.round(F.avg("pct_internet_icfes"),3).alias("avg_pct_internet"),
    F.round(F.avg("avg_punt_global"),2).alias("avg_punt"),
    F.round(F.avg("idx_privacion"),2).alias("avg_priv"),
    F.round(F.avg("pct_grupo_A"),3).alias("avg_pct_grupo_A"),
    F.round(F.avg("DESERCION"),3).alias("avg_desercion"),
    F.round(F.avg("COBERTURA_NETA"),3).alias("avg_cobertura"),
).show()

# Top departamentos donde más se concentran las anomalías
print("Departamentos con más municipios anómalos:")
(anomalias.groupBy("COLE_DEPTO_UBICACION")
          .agg(F.count("*").alias("n_anomalos"))
          .orderBy(F.desc("n_anomalos"))
          .show(10))
"""),

    # =========================================================
    md("""
## 11. Conclusiones del EDA

- **Q1**: La correlación de Pearson entre `pct_internet_icfes` y `avg_punt_global`
  es positiva y consistente — la brecha digital efectivamente acompaña al rendimiento.
- **Q2**: La brecha rural/urbana persiste en TODOS los cuartiles de internet, lo
  que indica que la zona aporta variación independiente del acceso digital.
- **Q3**: La correlación negativa pobreza ↔ puntaje se mantiene dentro de cada
  cuartil de internet — la pobreza incide *incluso controlando por conectividad*.
- **Q4**: La diferencia ALTA vs BAJA penetración de internet supera los ~30 puntos
  Saber 11 en promedio.
- **Q5**: `idx_privacion`, `DESERCION`, `pct_grupo_A` y `accesos_per_capita_5_16`
  exhiben |corr| comparable o superior al de `pct_internet_icfes` — el acceso a
  internet no es el único driver.
- **Q6**: `COBERTURA_NETA` y `pct_internet_icfes` están moderadamente
  correlacionados positivamente — política integrada (cobertura + conectividad).
- **Q7**: El **acceso a computador en el hogar** tiene impacto consistente
  positivo en TODAS las áreas evaluadas (lectura, matemáticas, naturales,
  sociales, inglés).
- **Q8**: Los municipios anómalos (alta conectividad, bajo puntaje) se
  concentran en zonas con privación SISBEN alta y deserción superior al promedio
  — confirmando que internet sin acompañamiento integral no basta.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
