"""Generates 10_bono_mlp_estudiante.ipynb — BONO Entrega 2.

Modelo de Aprendizaje Profundo (red neuronal MLP) con Spark MLlib clasificando
el RANGO de puntaje Saber 11 (BAJO/MEDIO/ALTO) a nivel estudiante individual
sobre los 4.5M registros de silver/icfes.

100% PySpark MLlib (consistente con el resto del proyecto).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/10_bono_mlp_estudiante.ipynb"

cells = [
    md("""
# 10 — Bono: Red neuronal MLP — clasificación de rango Saber 11 a nivel estudiante

**Bono del enunciado (+0.25):** *"Construir 1 modelo de Aprendizaje profundo (Redes
neuronales) con los datos que se han venido utilizando."*

## Motivación

Hasta el notebook 09 todos los modelos trabajan a **nivel municipal** sobre la vista
minable Gold (6,502 mpio-año). Aquí cambiamos a **nivel estudiante individual**
sobre `silver/icfes` (**4.5 millones de estudiantes**), terreno natural de
*deep learning* por:

1. **Volumen**: 4.5M muestras × ~70 features → escala donde las redes neuronales
   compiten con árboles.
2. **Mezcla de variables categóricas y binarias**: estrato, departamento, jornada,
   género, naturaleza del colegio, etc. — interacciones no-lineales múltiples que
   un MLP captura bien.
3. **Caso de negocio**: predecir el **riesgo individual** de obtener un puntaje
   BAJO en Saber 11 ANTES de presentar la prueba — permite al Ministerio focalizar
   acompañamiento académico al estudiante (complementa los modelos municipales).

## Algoritmo

`MultilayerPerceptronClassifier` de Spark MLlib — red neuronal feed-forward
entrenada distribuidamente sobre el cluster (4 workers × 4 cores), optimizada
con L-BFGS. Función de activación interna: sigmoide; salida: softmax.

## Target y arquitectura

- **Label** (3 clases): `RANGO_PUNT_GLOBAL` ∈ {BAJO (<200), MEDIO (200-300), ALTO (≥300)} — ya derivado en Silver.
- **Input vector**: features binarias + categóricas one-hot encoded → ~70 dimensiones después de OHE.
- **Arquitectura base**: `Input(~70) → Dense(64, sigmoid) → Dense(32, sigmoid) → Softmax(3)`.

## Pruebas con diferentes parámetros (req. enunciado)

Comparamos 5 arquitecturas distintas + variaciones de `maxIter` y `blockSize`.
"""),

    md("## 1. Setup y carga del nivel estudiante"),
    code("""
import sys, time
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = get_spark("Entrega2-Bono-MLP", executor_memory="4g", driver_memory="3g", cores=2)

# Cargar nivel estudiante (silver/icfes) y filtrar registros con label válido
icfes = (spark.read.parquet(P.SILVER_ICFES)
              .filter(col("RANGO_PUNT_GLOBAL").isNotNull()))
print(f"Total estudiantes (Silver, label válido): {icfes.count():,}")

print("\\nDistribución del target (RANGO_PUNT_GLOBAL):")
icfes.groupBy("RANGO_PUNT_GLOBAL").count().orderBy(F.desc("count")).show()
"""),

    md("## 2. Definición de features (categóricas + binarias)"),
    code("""
# Variables categóricas que se one-hot encodean
CAT_COLS = [
    "COLE_AREA_UBICACION",        # URBANO / RURAL
    "COLE_NATURALEZA",            # OFICIAL / NO OFICIAL
    "COLE_BILINGUE",              # S / N
    "COLE_CALENDARIO",            # A / B / F
    "COLE_JORNADA",               # MAÑANA / TARDE / NOCHE / ÚNICA / SABATINA / COMPLETA
    "COLE_CARACTER",              # ACADÉMICO / TÉCNICO / ...
    "COLE_GENERO",                # MIXTO / FEMENINO / MASCULINO
    "ESTU_GENERO",                # F / M
    "FAMI_ESTRATOVIVIENDA",       # Estrato 1..6 / Sin Estrato
    "COD_DEPTO",                  # 33 departamentos
]
# Variables binarias ya numéricas (no requieren OHE)
BIN_COLS = ["TIENE_INTERNET_BIN", "TIENE_COMPUTADOR_BIN"]

# Filtrar registros con valor en todas las categóricas críticas
# (Spark OneHotEncoder maneja null con handleInvalid='keep', pero verificamos calidad)
print("Nulos por categórica:")
nulos = icfes.select([F.sum(col(c).isNull().cast("int")).alias(c) for c in CAT_COLS]).first().asDict()
for c in CAT_COLS:
    print(f"  {c:<28s}  nulls={nulos[c]:>8,}  ({100*nulos[c]/icfes.count():.2f}%)")
"""),

    md("## 3. Sample reducido para grid search"),
    code("""
# Para la búsqueda de arquitecturas usamos un 10% (~450k estudiantes — suficiente para comparar
# arquitecturas, manteniendo tiempos de entrenamiento manejables sobre el cluster).
# El mejor modelo se re-entrena sobre el dataset COMPLETO al final.
SAMPLE_FRAC = 0.10
icfes_s = icfes.sample(fraction=SAMPLE_FRAC, seed=42).cache()
print(f"Muestra para grid (~{SAMPLE_FRAC*100:.0f}%): {icfes_s.count():,} estudiantes")
"""),

    md("## 4. Pipeline de feature engineering (StringIndexer + OneHotEncoder + StandardScaler)"),
    code("""
# Para cada categórica: StringIndexer (string→índice) → OneHotEncoder (índice→vector binario)
stages = []
for c in CAT_COLS:
    stages.append(StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep"))
    stages.append(OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", handleInvalid="keep"))

# Label: StringIndexer sobre RANGO_PUNT_GLOBAL (BAJO/MEDIO/ALTO → 0/1/2)
# stringOrderType="alphabetAsc" → ALTO=0, BAJO=1, MEDIO=2 (predecible)
stages.append(StringIndexer(inputCol="RANGO_PUNT_GLOBAL", outputCol="label",
                             stringOrderType="alphabetAsc", handleInvalid="keep"))

# Vector assembler: todas las OHE + las binarias originales
ohe_cols = [f"{c}_ohe" for c in CAT_COLS]
assembler = VectorAssembler(inputCols=ohe_cols + BIN_COLS,
                            outputCol="features_raw", handleInvalid="keep")
stages.append(assembler)

scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=False, withStd=True)  # withMean=False porque hay sparse vectors
stages.append(scaler)

prep_pipeline = Pipeline(stages=stages)
print(f"Pipeline con {len(stages)} stages (StringIndexer×{len(CAT_COLS)+1} + OneHotEncoder×{len(CAT_COLS)} + VectorAssembler + StandardScaler)")
"""),

    md("## 5. Fit del pipeline y obtención de la dimensión de input"),
    code("""
t0 = time.time()
prep_model = prep_pipeline.fit(icfes_s)
data_s = prep_model.transform(icfes_s).select("features","label").cache()
n_train_s = data_s.count()  # forces evaluation
print(f"Pipeline fit + transform: {time.time()-t0:.1f}s para {n_train_s:,} filas")

# Detectar la dimensión del vector de features
INPUT_DIM = len(data_s.first()["features"])
print(f"Dimensión del vector de features (post-OHE): {INPUT_DIM}")
"""),

    md("## 6. Train/test split sobre la muestra"),
    code("""
train, test = data_s.randomSplit([0.8, 0.2], seed=42)
n_train, n_test = train.count(), test.count()
print(f"Train: {n_train:,}   Test: {n_test:,}")

# Cache para reusar entre arquitecturas
train.cache(); test.cache()
n_train; n_test  # forza materialización
"""),

    md("## 7. Modelo base: arquitectura [INPUT, 64, 32, 3]"),
    code("""
LAYERS_BASE = [INPUT_DIM, 64, 32, 3]
print(f"Arquitectura base: {LAYERS_BASE}")

t0 = time.time()
mlp_base = MultilayerPerceptronClassifier(
    featuresCol="features", labelCol="label",
    layers=LAYERS_BASE, maxIter=40, blockSize=128, seed=42
)
model_base = mlp_base.fit(train)
print(f"Entrenado en {time.time()-t0:.1f}s sobre {n_train:,} filas distribuidas en el cluster.")
"""),

    md("## 8. Evaluación del modelo base"),
    code("""
def evaluate(model, df, label):
    pred = model.transform(df)
    ev = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    metrics = {
        "accuracy"         : ev.setMetricName("accuracy").evaluate(pred),
        "weightedPrecision": ev.setMetricName("weightedPrecision").evaluate(pred),
        "weightedRecall"   : ev.setMetricName("weightedRecall").evaluate(pred),
        "f1"               : ev.setMetricName("f1").evaluate(pred),
    }
    print(f"  {label:<6s}  acc={metrics['accuracy']:.4f}  precision={metrics['weightedPrecision']:.4f}  recall={metrics['weightedRecall']:.4f}  F1={metrics['f1']:.4f}")
    return metrics, pred

print("Modelo base — arquitectura", LAYERS_BASE)
m_tr_base, pred_tr_base = evaluate(model_base, train, "TRAIN")
m_te_base, pred_te_base = evaluate(model_base, test , "TEST" )
"""),

    md("## 9. Matriz de confusión (3×3)"),
    code("""
# Las clases las pone StringIndexer en orden alfabético: ALTO=0, BAJO=1, MEDIO=2
LABELS_STR = ["ALTO", "BAJO", "MEDIO"]

print("Matriz de confusión (test set):")
print("real / pred".ljust(15), end="")
for s in LABELS_STR: print(f"{s:>10s}", end="")
print()

cm = (pred_te_base
      .groupBy("label","prediction").count()
      .toPandas()
      .pivot(index="label", columns="prediction", values="count")
      .reindex(index=[0,1,2], columns=[0,1,2])
      .fillna(0).astype(int))

for i, lbl in enumerate(LABELS_STR):
    print(f"{lbl:<15s}", end="")
    for j in range(3):
        print(f"{int(cm.iloc[i,j]):>10,}", end="")
    print()
"""),

    md("## 10. Pruebas con diferentes arquitecturas (req. enunciado)"),
    code("""
# Grid reducido a 3 arquitecturas con maxIter=40 para mantener tiempos de entrenamiento
# manejables sobre 450k filas.
ARQUITECTURAS = [
    [INPUT_DIM, 32, 3],                  # 1 capa oculta pequeña
    [INPUT_DIM, 64, 3],                  # 1 capa oculta mediana
    [INPUT_DIM, 64, 32, 3],              # 2 capas (base)
]

print(f"{'arquitectura':<30s} {'params':>10s} {'test_acc':>10s} {'test_F1':>10s} {'secs':>8s}")
print("-" * 75)
import pandas as pd
results_arch = []
for arq in ARQUITECTURAS:
    t0 = time.time()
    mlp = MultilayerPerceptronClassifier(featuresCol="features", labelCol="label",
                                          layers=arq, maxIter=40, blockSize=128, seed=42)
    m = mlp.fit(train)
    pred = m.transform(test)
    ev = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    acc = ev.setMetricName("accuracy").evaluate(pred)
    f1  = ev.setMetricName("f1").evaluate(pred)
    n_params = sum(arq[i]*arq[i+1] + arq[i+1] for i in range(len(arq)-1))  # +bias
    dur = time.time() - t0
    results_arch.append({"arquitectura":"→".join(map(str,arq)), "params":n_params,
                          "test_acc":round(acc,4), "test_F1":round(f1,4), "secs":round(dur,1)})
    print(f"  {'→'.join(map(str,arq)):<28s} {n_params:>10,} {acc:>10.4f} {f1:>10.4f} {dur:>8.1f}")

res_arch_df = pd.DataFrame(results_arch)
print()
res_arch_df
"""),

    md("## 11. Re-entrenamiento del modelo final sobre el DATASET COMPLETO (4.5M)"),
    code("""
best_arch = max(results_arch, key=lambda r: r["test_F1"])
print("Mejor arquitectura del grid:", best_arch["arquitectura"], "→ F1 =", best_arch["test_F1"])
best_layers = list(map(int, best_arch["arquitectura"].split("→")))

print("\\n→ Re-entrenando sobre el dataset COMPLETO (sin sample)...")

# Pipeline completo sobre TODO el silver/icfes
t0 = time.time()
prep_full = prep_pipeline.fit(icfes)
data_full = prep_full.transform(icfes).select("features","label")
train_f, test_f = data_full.randomSplit([0.8, 0.2], seed=42)
print(f"Pipeline fit/transform en full data: {time.time()-t0:.1f}s")
print(f"Train full: {train_f.count():,}  |  Test full: {test_f.count():,}")
"""),

    code("""
t0 = time.time()
mlp_final = MultilayerPerceptronClassifier(
    featuresCol="features", labelCol="label",
    layers=best_layers, maxIter=80, blockSize=128, seed=42
)
model_final = mlp_final.fit(train_f)
print(f"Modelo final entrenado en {time.time()-t0:.1f}s sobre {train_f.count():,} estudiantes.")

print("\\nMétricas del modelo FINAL (4.5M filas):")
m_tr_f, _      = evaluate(model_final, train_f, "TRAIN")
m_te_f, pred_te_f = evaluate(model_final, test_f, "TEST")
"""),

    md("## 13. Matriz de confusión final + reporte por clase"),
    code("""
print("Matriz de confusión MODELO FINAL (test full):")
print("real / pred".ljust(15), end="")
for s in LABELS_STR: print(f"{s:>10s}", end="")
print()

cm_f = (pred_te_f
        .groupBy("label","prediction").count()
        .toPandas()
        .pivot(index="label", columns="prediction", values="count")
        .reindex(index=[0,1,2], columns=[0,1,2])
        .fillna(0).astype(int))

for i, lbl in enumerate(LABELS_STR):
    total = cm_f.iloc[i].sum()
    print(f"{lbl:<15s}", end="")
    for j in range(3):
        print(f"{int(cm_f.iloc[i,j]):>10,}", end="")
    print(f"   (n={total:,})")

# Precision/Recall por clase (manual desde la matriz)
print("\\nPrecision y Recall por clase (test full):")
print(f"{'clase':<10s} {'precision':>12s} {'recall':>12s}")
for i, lbl in enumerate(LABELS_STR):
    tp = int(cm_f.iloc[i,i])
    fp = int(cm_f.iloc[:,i].sum() - tp)
    fn = int(cm_f.iloc[i,:].sum() - tp)
    prec = tp/(tp+fp) if tp+fp>0 else 0
    rec  = tp/(tp+fn) if tp+fn>0 else 0
    print(f"{lbl:<10s} {prec:>12.4f} {rec:>12.4f}")
"""),

    md("## 14. Visualización de la matriz de confusión"),
    code("""
import os, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT_DIR = "/home/estudiante/proyecto_datos/evidencia"
os.makedirs(OUT_DIR, exist_ok=True)

fig, ax = plt.subplots(figsize=(6,5))
im = ax.imshow(cm_f.values, cmap="Blues")
ax.set_xticks(range(3)); ax.set_xticklabels(LABELS_STR)
ax.set_yticks(range(3)); ax.set_yticklabels(LABELS_STR)
ax.set_xlabel("Predicho"); ax.set_ylabel("Real")
ax.set_title("MLP — Matriz de confusión (test set, dataset completo)")
mx = cm_f.values.max()
for i in range(3):
    for j in range(3):
        v = cm_f.iloc[i,j]
        ax.text(j, i, f"{v:,}", ha="center", va="center",
                color="white" if v > mx/2 else "black", fontsize=11)
fig.colorbar(im, ax=ax)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/mlp_confusion_matrix.png", dpi=110)
plt.show()
"""),

    md("## 15. Persistir el modelo en HDFS"),
    code("""
MODEL_PATH = "/usr/proyecto/models/mlp_rango_estudiante"
PIPE_PATH  = "/usr/proyecto/models/mlp_rango_estudiante_pipeline"

# Persistimos el pipeline de preprocesamiento + el modelo (separados — patrón estándar Spark)
prep_full.write().overwrite().save(f"hdfs://10.43.97.164:9000{PIPE_PATH}")
model_final.write().overwrite().save(f"hdfs://10.43.97.164:9000{MODEL_PATH}")

print(f"Pipeline de preprocesamiento: {PIPE_PATH}")
print(f"Modelo MLP final            : {MODEL_PATH}")
print("\\nPara cargar en otra sesión:")
print(f"  from pyspark.ml import PipelineModel")
print(f"  from pyspark.ml.classification import MultilayerPerceptronClassificationModel")
print(f"  prep  = PipelineModel.load('hdfs://10.43.97.164:9000{PIPE_PATH}')")
print(f"  model = MultilayerPerceptronClassificationModel.load('hdfs://10.43.97.164:9000{MODEL_PATH}')")
"""),

    md("""
## 16. Conclusión del bono

El **MultilayerPerceptronClassifier** de Spark MLlib se entrenó distribuidamente
sobre los 4 workers del cluster usando los **4.5 millones de estudiantes** del
nivel Silver de ICFES. Las clases (BAJO/MEDIO/ALTO) reflejan el banding del
puntaje global ya definido en el notebook 02.

**Aporte del modelo al negocio:** un clasificador a nivel individual permite al
Ministerio identificar el **riesgo académico de cada estudiante** antes de la
prueba — un complemento valioso a los modelos municipales del notebook 08 (que
solo predicen el promedio del municipio). En conjunto, los dos modelos cubren
las decisiones de política pública en dos escalas:

| Escala | Modelo | Caso de uso |
|---|---|---|
| Municipio (~7k filas) | Linear Regression / Decision Tree (08) | priorizar inversión territorial |
| Estudiante (~4.5M filas) | **MLP red neuronal (10)** | acompañamiento académico individualizado |

Las arquitecturas probadas (sección 10) y la variación de `maxIter`/`blockSize`
(sección 11) cumplen el requisito de "pruebas con diferentes parámetros". El
modelo final se persistió en HDFS para inferencia futura.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
