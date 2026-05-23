# Plan de construcción del documento LaTeX — Entrega 2

> Repositorio LaTeX: `/Users/juanbaplo/ProcesamientoDatos/latex/`
> Documento base: `main.tex` (compila con `pdflatex main.tex`)
> Estrategia: iterativa, una sección a la vez, marcando ✓ a medida que se cierra.

## Decisiones de scope (confirmadas por el usuario)

1. **Sobreescribir** el documento E1 → E2 (el usuario tiene copia).
2. **Conservar** las secciones E1 (introducción, entendimiento de negocio, selección de datos, exploración, bonos E1, fuentes, repo, bibliografía) — solo darles una revisión ortográfica.
3. **Garantizar consistencia** en la grafía "Sisbén" (con tilde).
4. **Incluir tablas completas** de hiperparámetros (LR 12 combos, DT 15 combos, KMeans k=2..8, MLP arquitecturas).
5. **MLP bono**: esperar a que termine la ejecución para escribir esa sub-sección con métricas reales. Si toma demasiado, decidir si reducir el grid o reportar lo que quede.

---

## Fase 0 — Setup y limpieza de la base (≤15 min)

| ID | Tarea | Archivo(s) | Status |
|---|---|---|---|
| 0.1 | Cambiar título "Entrega 1" → "Entrega 2" en title page | `main.tex` | [ ] |
| 0.2 | Actualizar fecha del documento (mantener formato "Mes día año") | `main.tex` | [ ] |
| 0.3 | Corregir "SISBEN" → "Sisbén" en subsección `\subsection{Dataset de Personas SISBEN}` | `exploracion_limpieza_datos.tex` línea 40 | [ ] |
| 0.4 | Revisión ortográfica general de las secciones E1 (corrigir typos sin reescribir) | todas las E1 | [ ] |
| 0.5 | Agregar paquetes LaTeX necesarios para E2 (`longtable`, `caption`, `placeins`) en preámbulo | `main.tex` | [ ] |

---

## Fase 1 — Completar lo que faltaba de E1 (≤30 min)

| ID | Tarea | Archivo(s) | Status |
|---|---|---|---|
| 1.1 | Agregar subsección `\subsection{Dataset de Educación MEN}` con exploración del dataset MEN al final de `exploracion_limpieza_datos.tex` (E1 dejó esto pendiente) | `exploracion_limpieza_datos.tex` | [ ] |

Contenido sugerido para 1.1:
- Carga (15,707 registros × 41 columnas, 1,124 municipios, 2011-2024).
- Variables clave: cobertura neta/bruta, deserción, aprobación, sedes conectadas.
- Calidad: nulos por columna, duplicados confirmados por md5.
- Transformaciones iniciales planteadas: slugify de nombres, parse de porcentajes.

---

## Fase 2 — Nuevas secciones de Entrega 2 (núcleo, ~3-4 horas)

> Cada sección produce un `.tex` independiente en `sections/`, registrado en `main.tex`.

### 2.1 Infraestructura del cluster Big Data (~30 min)
**Archivo nuevo:** `sections/infraestructura_cluster.tex`
**Insertar en `main.tex` después de** `entendimiento_negocio.tex`.

Contenido:
- Diferencia con E1: en E1 se trabajó en Databricks; E2 migra a un cluster propio Hadoop+Spark.
- **Cluster MPDE12**: NameNode `10.43.97.164`, 4 workers (16 cores totales, 42 GB RAM).
- **Software**: Hadoop 3.3.6, Spark 3.5.2 standalone, JupyterLab.
- **Arquitectura medallion** Bronze → Silver → Gold (definir cada capa).
- **HDFS layout** `/usr/proyecto/{bronze,bronze_parquet,silver,gold,models}`.
- **Compresión** lograda: 4.8 GB CSV → 386 MB Parquet (~12×).
- **Vista minable** (Gold panel municipal): 7,004 filas × 44 columnas — base única de los modelos.
- Capturas/figuras: layout HDFS, Spark Master UI (cuando estén disponibles).

Status: [ ]

---

### 2.2 Filtros y transformaciones (numeral 1 del rúbrica) (~45 min)
**Archivo nuevo:** `sections/filtros_transformaciones.tex`

Contenido:
- Introducción: principio Bronze (cruda) vs Silver (limpia).
- Una subsección por dataset, cada una con DOS tablas (`booktabs`):
  - **Tabla A — Filtros aplicados**: # | Filtro | Justificación
  - **Tabla B — Transformaciones aplicadas**: # | Transformación | Justificación
- Datos a poblar (ya verificados en notebooks):
  - **ICFES** (notebook 02): 3 filtros (PUNT_GLOBAL not null, >0, <500) + 8 transformaciones (casts, binarias, geo, ANO, banding).
  - **Internet** (notebook 03): 2 filtros (NUM_ACCESOS válido, VELOCIDAD_BAJADA > 0) + 6 transformaciones (renombres, casts, parse coma decimal, lpad, normalización, particionado).
  - **Sisbén** (notebook 04): 2 filtros (cod_mpio not null, Grupo IN A/B/C/D) + 5 transformaciones (casts I1..I15, lpad, normalización, agregación masiva persona→municipio, idx_privacion).
  - **MEN** (notebook 05): 2 filtros (COD_MPIO+ANO not null, POBLACION_5_16>0) + 5 transformaciones (slugify, casts, parse "%" → double, lpad, particionado).
- Resumen final: tabla con conteo filas antes/después por dataset (ICFES: 7.1M→4.5M; Internet: 2.79M→2.79M; Sisbén: 4.47M→1,099 mpio; MEN: 15,707→15,700).

Status: [ ]

---

### 2.3 Respuesta a las 8 preguntas de negocio (numeral 2) (~60 min)
**Archivo nuevo:** `sections/respuesta_preguntas_negocio.tex`

Una subsección por pregunta, cada una con:
- Texto introductorio (1 párrafo): qué se midió y cómo.
- Tabla/número clave en `booktabs`.
- `\begin{figure}` con la imagen (cuando exista).
- Interpretación de negocio (1-2 párrafos).

Preguntas y assets:
| # | Pregunta | Figura disponible |
|---|---|---|
| Q1 | Internet en hogar vs Saber 11 | `figures/q1_internet_vs_puntaje.png` |
| Q2 | Rural vs urbano × conectividad | `figures/q2_zona_x_internet.png` |
| Q3 | Pobreza incide controlando por internet (tabla por cuartil) | tabla de texto |
| Q4 | Alta vs baja penetración (diferencia 22 pts) | tabla |
| Q5 | Variables socioeconómicas vs internet (ranking correlaciones) | tabla |
| Q6 | Cobertura educativa vs conectividad | tabla |
| Q7 | Acceso a computador por área evaluada | `figures/q7_computador_por_area.png` |
| Q8 | Municipios anómalos (10) | tabla |

Subtarea 2.3.a: copiar las 5 PNG (`q1`, `q2`, `q3` si se generó nuevo, `q7`, kmeans) de `proyecto/evidencia/` a `latex/figures/`.

Status: [ ]

---

### 2.4 Selección de técnicas de Aprendizaje de Máquina (numeral 3) (~20 min)
**Archivo nuevo:** `sections/seleccion_tecnicas_ml.tex`

Contenido:
- Justificación de **Linear Regression** y **Decision Tree Regressor** (supervisados — ambos cubiertos en el notebook de clase, complementan árboles vs lineal).
- Justificación de **K-Means** (no supervisado — para descubrir tipologías municipales).
- Por qué se descartaron Random Forest, MLP, etc. (alcance del notebook de clase).
- Alineación con el objetivo del negocio definido en E1.

Status: [ ]

---

### 2.5 Preparación de datos para modelado (numeral 4) (~40 min)
**Archivo nuevo:** `sections/preparacion_modelado.tex`

Subsecciones (4.a, 4.b, 4.c del rúbrica):
- **4.a Análisis de correlación y eliminación de redundantes**:
  - Texto: enfoque Pearson sobre features del panel Gold.
  - Tabla: pares con |r|>0.85 detectados y decisión (mantener X, descartar Y).
  - Figura: `figures/heatmap_correlaciones_final.png` (regenerar si necesario).
- **4.b Normalización de variables numéricas**:
  - `StandardScaler` de Spark MLlib (withMean=True, withStd=True).
  - Por qué es crítico para K-Means (sensible a escala) y benéfico para LR.
- **4.c Selección de variables por criterio de negocio**:
  - Tabla agrupada por dimensión: Conectividad / Pobreza / Educación / Tamaño-Demografía.
  - Imputación con mediana (`approxQuantile` + `fillna`).

Status: [ ]

---

### 2.6 Resultados del modelado (numerales 5 y 6) (~60 min)
**Archivo nuevo:** `sections/resultados_modelado.tex`

Sub-secciones:
- **Pipeline común**: `VectorAssembler → StandardScaler → modelo`.
- **Regresión Lineal**:
  - Tabla completa de 12 combos (regParam × elasticNetParam) con TRAIN_R2, TEST_R2, TEST_RMSE.
  - Coeficientes del mejor modelo.
- **Decision Tree Regressor**:
  - Tabla completa de 15 combos (maxDepth × minInstancesPerNode).
  - Importancia de variables (gráfico de barras).
- **Comparativa final** (estilo clase 4.4): tabla `Modelo | TRAIN_R² | TEST_R² | TEST_MAE | TEST_RMSE`.
- **K-Means**:
  - Tabla completa k=2..8 (WCSS + silueta).
  - Figura: `figures/kmeans_eleccion_k.png`.
  - Perfilado de clusters (tabla con n_mpio, avg_punt, pct_internet, idx_priv, etc.).
  - Figura: `figures/kmeans_clusters_scatter.png`.
- **Persistencia en HDFS** de los modelos (rutas `/usr/proyecto/models/...`).

Subtarea 2.6.a: copiar PNGs de KMeans desde `evidencia/` a `figures/`.

Status: [ ]

---

### 2.7 Conclusiones y recomendaciones de negocio (~30 min)
**Archivo nuevo:** `sections/conclusiones.tex`

Contenido (cierre CRISP-DM):
- Síntesis de hallazgos de las 8 preguntas (1 párrafo agregado).
- Hallazgos de los modelos (LR vs DT, qué predice mejor, importancia de features).
- Tipologías KMeans → identificación del grupo objetivo del plan de acción.
- Recomendaciones concretas para el MEN:
  1. Política focalizada en municipios cluster con privación alta + conectividad baja.
  2. Conectividad de calidad (velocidad, no solo acceso).
  3. Acompañamiento integral (no solo infraestructura).
  4. Sistema de alerta temprana a nivel estudiante (cuando exista el MLP).
- Limitaciones del análisis y trabajo futuro.

Status: [ ]

---

## Fase 3 — Bono: subsección MLP (cuando termine la ejecución) (~25 min)

| ID | Tarea | Archivo(s) | Status |
|---|---|---|---|
| 3.1 | Esperar a que termine la ejecución del notebook 10_bono_mlp_estudiante.ipynb | log `/tmp/nb_logs/10_bono_mlp.log` | [ ] |
| 3.2 | Si toma más de 90 min: parar y reducir el grid de arquitecturas (de 5 a 3) y maxIter (de 150 a 60) | spec_10 | [ ] |
| 3.3 | Extraer métricas reales (accuracy, F1, matriz de confusión) y copiar `mlp_confusion_matrix.png` | — | [ ] |
| 3.4 | Agregar subsección `\subsection{Modelo de Aprendizaje Profundo (MLP)}` a `bonos.tex` con: motivación, arquitectura, tabla de combos probados, matriz de confusión, conclusión de negocio | `bonos.tex` | [ ] |

---

## Fase 4 — Cierres y consistencia (~20 min)

| ID | Tarea | Archivo(s) | Status |
|---|---|---|---|
| 4.1 | Actualizar `main.tex` con todos los `\input` nuevos en orden correcto | `main.tex` | [ ] |
| 4.2 | Agregar 4-5 referencias nuevas a `bibliografia.tex`: Apache Spark, Spark MLlib, Random Forest (Breiman), K-Means (MacQueen), MLP (Rumelhart/Hinton si se mantiene el bono) | `bibliografia.tex` | [ ] |
| 4.3 | Compilar el documento end-to-end (`pdflatex main.tex && pdflatex main.tex` para resolver referencias cruzadas) y verificar que no hay warnings/errores | — | [ ] |
| 4.4 | Revisión final ortográfica (corrigir cualquier "SISBEN" residual a "Sisbén", normalizar mayúsculas, verificar comillas y tildes) | todos los `.tex` | [ ] |
| 4.5 | Validar que el orden de figuras es coherente y los labels coinciden con el texto | — | [ ] |

---

## Fase 5 — Capturas Spark/HDFS UI (opcional, dependiente del usuario)

Estas las debe tomar el usuario desde su navegador (ver `proyecto/CAPTURAS_UI.md`).
Si las pega en `latex/figures/`, las incluyo en la sección 2.1 (Infraestructura).

| ID | Tarea | Status |
|---|---|---|
| 5.1 | Captura `spark_master_overview.png` (workers + cores) | [ ] (usuario) |
| 5.2 | Captura `spark_app_jobs.png` (jobs corriendo) | [ ] (usuario) |
| 5.3 | Captura `spark_app_stages.png` (stages paralelizando) | [ ] (usuario) |
| 5.4 | Captura `spark_app_executors.png` (workers reales) | [ ] (usuario) |
| 5.5 | Captura `hdfs_overview.png` y `hdfs_browse_proyecto.png` | [ ] (usuario) |

---

## Orden recomendado de ejecución (iterativo)

```
Fase 0 (setup)        ── Fase 1 (MEN E1) ── Fase 2.1 (cluster) ── Fase 2.2 (filtros)
                                                                         │
Fase 2.3 (preguntas) ── Fase 2.4 (selección ML) ── Fase 2.5 (preparación)│
                                                                         │
Fase 2.6 (resultados) ── Fase 2.7 (conclusiones) ── Fase 3 (MLP si está) │
                                                                         │
                                              Fase 4 (cierre y compile)  ┘
```

Después de cada fase: commit/sync con el usuario para validar antes de avanzar.

---

## Notas de mantenimiento de este plan

- A medida que se complete cada item, cambiar `[ ]` por `[x]` en este archivo.
- Si surgen nuevas tareas durante la ejecución, agregarlas al final con un ID consecutivo.
- Si se cambia de scope con el usuario, actualizar la sección "Decisiones de scope".
