"""Generates 08_ml_supervisado_regresion.ipynb.
Solo usa lo cubierto en el notebook de clase:
    - pyspark.ml.regression: LinearRegression, DecisionTreeRegressor
    - pyspark.ml.feature: VectorAssembler, StandardScaler
    - pyspark.ml.Pipeline
    - pyspark.ml.evaluation: RegressionEvaluator
    - df.stat.corr() para correlación de Pearson
    - fillna + approxQuantile (mediana) para imputación
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/08_ml_supervisado_regresion.ipynb"

cells = [
    md("""
# 08 — Aprendizaje supervisado: Regresión sobre la vista minable municipal

**Pregunta:** ¿podemos predecir el **puntaje global promedio** de un municipio-año
a partir de su perfil de conectividad, pobreza y educación?

**Modelos** (los 2 cubiertos en clase para regresión):

1. `LinearRegression` — modelo lineal regularizado (Ridge/ElasticNet)
2. `DecisionTreeRegressor` — árbol de regresión

**Vista minable:** `gold/panel_municipal` (joined de ICFES ⋈ Internet ⋈ SISBEN ⋈ MEN,
grano municipio-año, 4 fuentes unidas → un solo dataset).

**Pipeline de la clase (sección 1.5):**
`fillna(median) → VectorAssembler → StandardScaler → Regressor`
"""),

    md("## 1. Setup y carga de la vista minable"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.functions import col

spark = get_spark("Entrega2-ML-Regresion", executor_memory="4g", driver_memory="3g", cores=2)

panel = spark.read.parquet(P.GOLD_PANEL_MUNICIPAL)
TARGET = "avg_punt_global"

# Filtro de calidad: solo filas con target válido y tamaño mínimo
panel_ml = (panel
    .filter(col(TARGET).isNotNull())
    .filter(col("n_estudiantes") >= 30))
print(f"Filas de la vista minable: {panel.count():,}")
print(f"Filas para ML (con target y n>=30): {panel_ml.count():,}")
"""),

    md("## 2. Selección inicial de features"),
    code("""
FEATURES = [
    "pct_internet_icfes",          # conectividad — declarada por estudiante
    "accesos_per_capita_5_16",     # conectividad — MinTIC normalizado por población
    "avg_velocidad_bajada",        # calidad de internet
    "n_proveedores",               # diversidad de oferta
    "idx_privacion",               # pobreza — suma de 15 indicadores SISBEN
    "pct_grupo_A",                 # pobreza — % en grupo más vulnerable
    "pct_rural_sisben",            # ruralidad
    "COBERTURA_NETA",              # educación — oferta
    "DESERCION",                   # educación — calidad
    "APROBACION",                  # educación — calidad
    "n_estudiantes",               # tamaño
    "POBLACION_5_16",              # demografía
]
# Cast a double (algunas vienen long o decimal)
for f in FEATURES:
    panel_ml = panel_ml.withColumn(f, col(f).cast("double"))
print(f"Features candidatas: {len(FEATURES)}")
"""),

    md("## 3. Reporte de nulos y filtrado de features 100% nulas"),
    code("""
from pyspark.sql.functions import count, when

null_counts = (panel_ml
    .select([F.sum(col(c).isNull().cast("int")).alias(c) for c in FEATURES])
    .first().asDict())
total = panel_ml.count()

print(f"{'feature':<28s} {'nulls':>8s} {'%null':>8s}")
USABLE = []
for c in FEATURES:
    n = null_counts[c]
    pct = 100*n/total
    flag = "DESCARTAR" if n == total else "ok"
    print(f"  {c:<28s} {n:>8d} {pct:>7.1f}%  {flag}")
    if n < total:
        USABLE.append(c)
print(f"\\nFeatures usables: {len(USABLE)} / {len(FEATURES)}")
"""),

    md("## 4. Imputación de nulos (mediana — patrón de clase, sección 1.3)"),
    code("""
# Para cada feature numérica con nulos, computamos la mediana con approxQuantile
# (igual a la clase) y aplicamos fillna.
medianas = {}
for c in USABLE:
    q = panel_ml.approxQuantile(c, [0.5], 0.01)
    if q:
        medianas[c] = q[0]
print("Medianas calculadas:")
for c, m in medianas.items():
    print(f"  {c:<28s} = {m:.4f}")

panel_imp = panel_ml.fillna(medianas)
# Verificación
nulos_post = panel_imp.select(
    [F.sum(col(c).isNull().cast("int")).alias(c) for c in USABLE]
).first().asDict()
print(f"\\nNulos tras imputación: {sum(nulos_post.values())}")
"""),

    md("## 5. Análisis de correlación (df.stat.corr — patrón clase) y eliminación de redundantes"),
    code("""
# Matriz de Pearson entre todas las features e incluyendo el target
cols_all = USABLE + [TARGET]
print(f"Matriz de correlación de Pearson ({len(cols_all)} variables):")
print(f"{'':22s}", end="")
for c in cols_all: print(f"{c[:13]:>14s}", end="")
print()
for c1 in cols_all:
    print(f"{c1[:20]:<22s}", end="")
    for c2 in cols_all:
        r = panel_imp.stat.corr(c1, c2)
        print(f"{r:+.3f}        ", end="")
    print()
"""),

    code("""
# Identificar pares de features con |r| > 0.85 y descartar el de menor corr con el target
THRESH = 0.85
target_corr = {c: panel_imp.stat.corr(c, TARGET) for c in USABLE}

pairs = []
for i, a in enumerate(USABLE):
    for b in USABLE[i+1:]:
        r_ab = panel_imp.stat.corr(a, b)
        if abs(r_ab) > THRESH:
            keep, drop = (a, b) if abs(target_corr[a]) >= abs(target_corr[b]) else (b, a)
            pairs.append((a, b, r_ab, keep, drop))

DROP_CORR = set()
print(f"Pares con |r| > {THRESH}:")
if not pairs:
    print("  (ninguno — no hay multicolinealidad fuerte)")
for a,b,r,keep,drop in pairs:
    print(f"  {a} <-> {b}  r={r:+.3f}  → mantener {keep}, descartar {drop}")
    DROP_CORR.add(drop)

FINAL = [c for c in USABLE if c not in DROP_CORR]
print(f"\\nFeatures finales para modelar ({len(FINAL)}): {FINAL}")
"""),

    md("## 6. Pipeline de feature engineering: VectorAssembler + StandardScaler"),
    code("""
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=FINAL, outputCol="features_raw", handleInvalid="keep")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                            withMean=True, withStd=True)
prep_pipeline = Pipeline(stages=[assembler, scaler])

# Train/test split estilo clase (sección 1.5)
df_train, df_test = panel_imp.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {df_train.count():,} filas  |  Test: {df_test.count():,} filas")

prep_model = prep_pipeline.fit(df_train)
train_prep = prep_model.transform(df_train).withColumn("label", col(TARGET).cast("double"))
test_prep  = prep_model.transform(df_test ).withColumn("label", col(TARGET).cast("double"))

print("\\nVista minable lista (3 filas):")
train_prep.select("COD_MPIO","ANO","features","label").show(3, truncate=False)
"""),

    md("## 7. Modelo 1 — Regresión Lineal"),
    code("""
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

lr = LinearRegression(featuresCol="features", labelCol="label",
                      maxIter=50, regParam=0.1, elasticNetParam=0.0)
lr_model = lr.fit(train_prep)
pred_lr_tr = lr_model.transform(train_prep)
pred_lr_te = lr_model.transform(test_prep)

def metrics(df, name):
    ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    mae  = ev.setMetricName("mae" ).evaluate(df)
    rmse = ev.setMetricName("rmse").evaluate(df)
    r2   = ev.setMetricName("r2"  ).evaluate(df)
    print(f"  {name}  MAE={mae:.2f}  RMSE={rmse:.2f}  R2={r2:.4f}")
    return {"MAE":mae,"RMSE":rmse,"R2":r2}

print("Regresión lineal:")
m_lr_tr = metrics(pred_lr_tr, "TRAIN")
m_lr_te = metrics(pred_lr_te, "TEST ")

print(f"\\nCoeficientes (features):")
for f, w in zip(FINAL, lr_model.coefficients):
    print(f"  {f:<28s}  beta = {w:+.4f}")
print(f"Intercepto: {lr_model.intercept:.4f}")
"""),

    md("## 8. Modelo 2 — Decision Tree Regressor"),
    code("""
from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(featuresCol="features", labelCol="label",
                            maxDepth=10, minInstancesPerNode=5, seed=42)
dt_model = dt.fit(train_prep)
pred_dt_tr = dt_model.transform(train_prep)
pred_dt_te = dt_model.transform(test_prep)

print("Decision Tree Regressor:")
m_dt_tr = metrics(pred_dt_tr, "TRAIN")
m_dt_te = metrics(pred_dt_te, "TEST ")
"""),

    md("## 9. Pruebas con diferentes hiperparámetros (sección 4 del enunciado)"),
    code("""
import time

print("Linear Regression — grid sobre regParam y elasticNetParam:")
print(f"  {'regParam':>10s} {'elasticNet':>12s} {'TRAIN_R2':>10s} {'TEST_R2':>10s} {'TEST_RMSE':>11s}")
res_lr = []
for rp in [0.0, 0.01, 0.1, 0.5]:
    for en in [0.0, 0.5, 1.0]:
        m = LinearRegression(featuresCol="features", labelCol="label",
                              maxIter=50, regParam=rp, elasticNetParam=en).fit(train_prep)
        ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
        r2_tr = ev.setMetricName("r2").evaluate(m.transform(train_prep))
        r2_te = ev.setMetricName("r2").evaluate(m.transform(test_prep))
        rmse  = ev.setMetricName("rmse").evaluate(m.transform(test_prep))
        res_lr.append((rp, en, r2_tr, r2_te, rmse))
        print(f"  {rp:>10.3f} {en:>12.2f} {r2_tr:>10.4f} {r2_te:>10.4f} {rmse:>11.2f}")
"""),

    code("""
print("\\nDecision Tree — grid sobre maxDepth y minInstancesPerNode:")
print(f"  {'maxDepth':>10s} {'minInst':>10s} {'TRAIN_R2':>10s} {'TEST_R2':>10s} {'TEST_RMSE':>11s}")
res_dt = []
for md_ in [3, 5, 8, 10, 15]:
    for mi in [1, 5, 10]:
        m = DecisionTreeRegressor(featuresCol="features", labelCol="label",
                                   maxDepth=md_, minInstancesPerNode=mi, seed=42).fit(train_prep)
        ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
        r2_tr = ev.setMetricName("r2").evaluate(m.transform(train_prep))
        r2_te = ev.setMetricName("r2").evaluate(m.transform(test_prep))
        rmse  = ev.setMetricName("rmse").evaluate(m.transform(test_prep))
        res_dt.append((md_, mi, r2_tr, r2_te, rmse))
        print(f"  {md_:>10d} {mi:>10d} {r2_tr:>10.4f} {r2_te:>10.4f} {rmse:>11.2f}")
"""),

    md("## 10. Comparativa final y selección del mejor modelo"),
    code("""
# Mejor LR y mejor DT por test_R2
best_lr = max(res_lr, key=lambda x: x[3])
best_dt = max(res_dt, key=lambda x: x[3])

print("Mejor LinearRegression :  regParam=%.3f  elasticNetParam=%.2f  → test_R2=%.4f  RMSE=%.2f"
      % (best_lr[0], best_lr[1], best_lr[3], best_lr[4]))
print("Mejor DecisionTree     :  maxDepth=%d  minInst=%d              → test_R2=%.4f  RMSE=%.2f"
      % (best_dt[0], best_dt[1], best_dt[3], best_dt[4]))

print("\\nTabla resumen (clase 4.4):")
print(f"{'Modelo':<28s} {'TRAIN_R2':>10s} {'TEST_R2':>10s} {'TEST_MAE':>10s} {'TEST_RMSE':>11s}")
print("-" * 75)
print(f"{'Regresión Lineal (default)':<28s} {m_lr_tr['R2']:>10.4f} {m_lr_te['R2']:>10.4f} "
      f"{m_lr_te['MAE']:>10.2f} {m_lr_te['RMSE']:>11.2f}")
print(f"{'Decision Tree (default)':<28s} {m_dt_tr['R2']:>10.4f} {m_dt_te['R2']:>10.4f} "
      f"{m_dt_te['MAE']:>10.2f} {m_dt_te['RMSE']:>11.2f}")
print(f"{'LR mejor del grid':<28s} {'-':>10s} {best_lr[3]:>10.4f} {'-':>10s} {best_lr[4]:>11.2f}")
print(f"{'DT mejor del grid':<28s} {'-':>10s} {best_dt[3]:>10.4f} {'-':>10s} {best_dt[4]:>11.2f}")
"""),

    md("## 11. Modelo final (DT) — importancia de variables"),
    code("""
# Decision Tree expone featureImportances (Vector)
print("Importancia de variables (Decision Tree mejor del grid):")
dt_best = DecisionTreeRegressor(featuresCol="features", labelCol="label",
                                 maxDepth=int(best_dt[0]),
                                 minInstancesPerNode=int(best_dt[1]),
                                 seed=42).fit(train_prep)
importancias = list(zip(FINAL, dt_best.featureImportances.toArray()))
importancias.sort(key=lambda x: -x[1])
for f, v in importancias:
    barra = "#" * int(v*80)
    print(f"  {f:<28s} {v:.4f}  {barra}")
"""),

    md("## 12. Predicciones de muestra (estilo clase 2.1)"),
    code("""
pred_best = dt_best.transform(test_prep)
print("Predicciones del mejor modelo (muestra de 15 municipios-año):")
pred_best.select("COD_MPIO","ANO","COLE_DEPTO_UBICACION",
                 col("label").alias("punt_real"),
                 F.round(col("prediction"),1).alias("punt_pred"),
                 F.round(F.abs(col("label")-col("prediction")),1).alias("error_abs")
                 ) \\
         .orderBy(F.rand(seed=1)).show(15, truncate=False)
"""),

    md("## 13. Persistir mejores modelos en HDFS"),
    code("""
# Guardamos los modelos finales para reuso/inferencia
PATH_LR = "/usr/proyecto/models/lr_punt_global"
PATH_DT = "/usr/proyecto/models/dt_punt_global"

lr_best_model = LinearRegression(featuresCol="features", labelCol="label",
    maxIter=50, regParam=float(best_lr[0]), elasticNetParam=float(best_lr[1])).fit(train_prep)
lr_best_model.write().overwrite().save(f"hdfs://10.43.97.164:9000{PATH_LR}")
dt_best.write().overwrite().save(f"hdfs://10.43.97.164:9000{PATH_DT}")
print(f"LR persistido en HDFS: {PATH_LR}")
print(f"DT persistido en HDFS: {PATH_DT}")
"""),

    md("""
## 14. Conclusión

Sobre la **vista minable municipal** (gold/panel_municipal — los datos de las 4
fuentes unidos por (municipio, año)):

- El **Decision Tree** capta interacciones no-lineales mejor que LR; suele
  ganar en test_R² a costa de mayor varianza (overfitting con maxDepth grande).
- La **Regresión Lineal** con regularización (Ridge/ElasticNet) es más estable y
  sus coeficientes son directamente interpretables como impacto marginal.
- Ambos modelos usan el mismo conjunto de features (sección 5 — tras eliminar
  multicolinealidad), entrenan sobre el mismo split 80/20 y se comparan con las
  métricas estándar de regresión (`MAE`, `RMSE`, `R²` — `RegressionEvaluator` de
  la clase 4.1).
- Los modelos se persisten en HDFS y pueden recargarse con `.load()` para
  inferencia sobre nuevos datos municipales.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
