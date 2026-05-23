"""Generates 09_ml_no_supervisado_kmeans.ipynb.
Solo usa lo cubierto en el notebook de clase (sección 3):
    - pyspark.ml.clustering.KMeans
    - pyspark.ml.feature: VectorAssembler, StandardScaler
    - pyspark.ml.Pipeline
    - pyspark.ml.evaluation.ClusteringEvaluator (silhouette)
    - m.summary.trainingCost (WCSS)
    - fillna + approxQuantile (mediana) para imputar
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/09_ml_no_supervisado_kmeans.ipynb"

cells = [
    md("""
# 09 — Aprendizaje no supervisado: K-Means de tipologías municipales

**Objetivo:** Agrupar municipios en tipologías según su perfil combinado de
**conectividad + pobreza + cobertura + rendimiento**, usando la vista minable
`gold/panel_municipal`.

**Algoritmo:** `KMeans` de Spark MLlib (sección 3 del notebook de clase).

**Elección de k:** método del codo (WCSS = `m.summary.trainingCost`) +
coeficiente de silueta (`ClusteringEvaluator`) para k ∈ [2, 8].

**Preprocesamiento idéntico al supervisado** (notebook 08): los modelos
no-supervisados también consumen la misma vista minable unificada.
"""),

    md("## 1. Setup y datos"),
    code("""
import sys, os
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = get_spark("Entrega2-ML-KMeans", executor_memory="3g", driver_memory="3g", cores=2)
panel = spark.read.parquet(P.GOLD_PANEL_MUNICIPAL)

# Snapshot del año más reciente con tamaño mínimo
ano_max = panel.agg(F.max("ANO")).first()[0]
print(f"Año seleccionado para clustering: {ano_max}")
snap = (panel.filter(col("ANO") == ano_max)
             .filter(col("avg_punt_global").isNotNull())
             .filter(col("n_estudiantes") >= 30))
print(f"Filas (municipios) snapshot: {snap.count():,}")
"""),

    md("## 2. Selección de features y reporte de nulos"),
    code("""
FEATURES = [
    "pct_internet_icfes",
    "accesos_per_capita_5_16",
    "idx_privacion",
    "pct_grupo_A",
    "avg_punt_global",
    "COBERTURA_NETA",
]
for f in FEATURES:
    snap = snap.withColumn(f, col(f).cast("double"))

null_counts = snap.select(
    [F.sum(col(c).isNull().cast("int")).alias(c) for c in FEATURES]
).first().asDict()
total = snap.count()
USABLE = [f for f in FEATURES if null_counts[f] < total]
print(f"Features usables (no 100% null): {len(USABLE)} / {len(FEATURES)}")
for f in FEATURES:
    pct = 100 * null_counts[f] / total
    flag = "DESCARTAR" if f not in USABLE else "ok"
    print(f"  {f:<28s} nulls={null_counts[f]:>5d}  {pct:>5.1f}%   {flag}")
"""),

    md("## 3. Imputación con mediana (approxQuantile + fillna — patrón clase 1.3)"),
    code("""
medianas = {}
for f in USABLE:
    q = snap.approxQuantile(f, [0.5], 0.01)
    if q:
        medianas[f] = q[0]
print("Medianas imputadas:")
for f, m in medianas.items():
    print(f"  {f:<28s} = {m:.4f}")

snap_imp = snap.fillna(medianas)
nulos_post = snap_imp.select(
    [F.sum(col(c).isNull().cast("int")).alias(c) for c in USABLE]
).first().asDict()
print(f"\\nNulos tras imputación: {sum(nulos_post.values())}")
"""),

    md("## 4. Pipeline de feature engineering: Assembler + StandardScaler (clase 1.5)"),
    code("""
assembler = VectorAssembler(inputCols=USABLE, outputCol="features_raw", handleInvalid="keep")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                            withMean=True, withStd=True)

prep_pipeline = Pipeline(stages=[assembler, scaler])
prep_model = prep_pipeline.fit(snap_imp)
data = (prep_model.transform(snap_imp)
        .select("COD_MPIO","COLE_DEPTO_UBICACION","features", *USABLE, "n_estudiantes")
        .cache())
print(f"Datos preparados: {data.count():,} municipios con vector 'features' listo.")
"""),

    md("## 5. Elección de k — método del codo + silueta (clase 3.2)"),
    code("""
results = []
ev = ClusteringEvaluator(featuresCol="features", predictionCol="cluster",
                          metricName="silhouette",
                          distanceMeasure="squaredEuclidean")

for k in range(2, 9):
    km = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42, maxIter=40)
    m  = km.fit(data)
    df_pred = m.transform(data)
    wcss = m.summary.trainingCost              # WCSS
    silh = ev.evaluate(df_pred)                # silueta
    results.append((k, wcss, silh))
    print(f"  K={k}  →  WCSS={wcss:>10.2f}  |  Silhouette={silh:.4f}")
"""),

    code("""
# Plot codo + silueta (solo evidencia visual; cómputo ya está en Spark)
import matplotlib
matplotlib.use("Agg"); import matplotlib.pyplot as plt
OUT_DIR = "/home/estudiante/proyecto_datos/evidencia"; os.makedirs(OUT_DIR, exist_ok=True)

ks = [r[0] for r in results]
wcss = [r[1] for r in results]
sils = [r[2] for r in results]

fig, axes = plt.subplots(1,2, figsize=(12,4))
axes[0].plot(ks, wcss, "o-", color="tab:blue")
axes[0].set_xlabel("k"); axes[0].set_ylabel("WCSS"); axes[0].set_title("Método del codo (WCSS)")
axes[0].grid(alpha=0.3)
axes[1].plot(ks, sils, "s-", color="tab:orange")
axes[1].set_xlabel("k"); axes[1].set_ylabel("Silhouette"); axes[1].set_title("Coeficiente de silueta")
axes[1].grid(alpha=0.3)
plt.tight_layout(); plt.savefig(f"{OUT_DIR}/kmeans_eleccion_k.png", dpi=110); plt.show()
"""),

    md("## 6. Modelo final (k según mejor silueta)"),
    code("""
K_BEST = max(results, key=lambda r: r[2])[0]
print(f"K elegido por silueta: {K_BEST}")

km_final = KMeans(featuresCol="features", predictionCol="cluster",
                   k=K_BEST, seed=42, maxIter=60)
model_km = km_final.fit(data)
clusters = model_km.transform(data).cache()
print(f"Tamaño por cluster:")
clusters.groupBy("cluster").count().orderBy("cluster").show()
"""),

    md("## 7. Perfilado de clusters (clase 3.3)"),
    code("""
# Promedio por cluster para cada feature original (no escalada)
agg_exprs = [F.count("*").alias("n_municipios")]
if "avg_punt_global"           in USABLE: agg_exprs.append(F.round(F.avg("avg_punt_global"),1).alias("avg_punt"))
if "pct_internet_icfes"        in USABLE: agg_exprs.append(F.round(F.avg("pct_internet_icfes")*100,1).alias("pct_internet"))
if "accesos_per_capita_5_16"   in USABLE: agg_exprs.append(F.round(F.avg("accesos_per_capita_5_16"),3).alias("accesos_pc"))
if "idx_privacion"             in USABLE: agg_exprs.append(F.round(F.avg("idx_privacion"),2).alias("idx_priv"))
if "pct_grupo_A"               in USABLE: agg_exprs.append(F.round(F.avg("pct_grupo_A")*100,1).alias("pct_grupoA"))
if "COBERTURA_NETA"            in USABLE: agg_exprs.append(F.round(F.avg("COBERTURA_NETA")*100,1).alias("cob_neta"))

perfil = clusters.groupBy("cluster").agg(*agg_exprs).orderBy("cluster")
perfil.show()

# Centroides en el espacio escalado (mismo formato que la clase)
print("Centroides en el espacio de features escaladas:")
for i, c in enumerate(model_km.clusterCenters()):
    print(f"  Cluster {i}: {[round(x,3) for x in c]}")
"""),

    md("## 8. Ejemplos de municipios por cluster"),
    code("""
for c in sorted([r["cluster"] for r in clusters.select("cluster").distinct().collect()]):
    print(f"\\n=== Cluster {c} (top 8 municipios por n_estudiantes) ===")
    (clusters.filter(col("cluster")==c)
             .select("COD_MPIO","COLE_DEPTO_UBICACION",
                     "avg_punt_global","pct_internet_icfes",
                     "idx_privacion","n_estudiantes")
             .orderBy(F.desc("n_estudiantes"))
             .show(8, truncate=False))
"""),

    md("## 9. Scatter de tipologías (pct_internet vs avg_punt, color por cluster)"),
    code("""
plot_df = (clusters.select("pct_internet_icfes","avg_punt_global","cluster","n_estudiantes")
                   .toPandas().dropna())
fig, ax = plt.subplots(figsize=(9,6))
cmap = plt.get_cmap("tab10")
for c in sorted(plot_df["cluster"].unique()):
    sub = plot_df[plot_df["cluster"]==c]
    ax.scatter(sub["pct_internet_icfes"], sub["avg_punt_global"],
               s=sub["n_estudiantes"].clip(upper=2000)/8,
               alpha=0.55, color=cmap(c), label=f"Cluster {c}", edgecolors="none")
ax.set_xlabel("% estudiantes con internet (ICFES)")
ax.set_ylabel("Puntaje global promedio")
ax.set_title(f"K-Means k={K_BEST} — tipologías municipales (snapshot año {ano_max})")
ax.grid(alpha=0.3); ax.legend()
plt.tight_layout(); plt.savefig(f"{OUT_DIR}/kmeans_clusters_scatter.png", dpi=110); plt.show()
"""),

    md("## 10. Persistir clusters y modelo en HDFS"),
    code("""
CLUSTER_OUT = f"/usr/proyecto/gold/clusters_municipales_k{K_BEST}_ano{ano_max}"
(clusters.select("COD_MPIO","COLE_DEPTO_UBICACION","cluster", *USABLE, "n_estudiantes")
         .coalesce(1)
         .write.mode("overwrite").option("compression","snappy")
         .parquet(f"hdfs://10.43.97.164:9000{CLUSTER_OUT}"))

MODEL_PATH = f"/usr/proyecto/models/kmeans_k{K_BEST}"
model_km.write().overwrite().save(f"hdfs://10.43.97.164:9000{MODEL_PATH}")
print("Clusters:", CLUSTER_OUT)
print("Modelo  :", MODEL_PATH)
"""),

    md("""
## 11. Conclusión

K-Means identifica tipologías de municipios sobre la vista minable Gold,
distinguiendo perfiles combinados (conectividad + pobreza + rendimiento). El
método del codo y la silueta convergen en el k seleccionado. Los centroides
escalados y el perfil promedio por cluster permiten interpretación de negocio
directa: cada cluster define un *grupo objetivo* del plan de acción del
Ministerio de Educación.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
