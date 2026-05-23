# Resultados — Entrega 2 (Grupo REST pAPIs)
> Brecha digital y resultados Saber 11 en Colombia · Pipeline Hadoop HDFS + Apache Spark + MLlib

Ejecución realizada el **2026-05-19**, en el cluster `MPDE12` (4 workers · 16 cores · 42 GB RAM).
Todos los notebooks corrieron de punta a punta en ≈ 21 min.

---

## 1. Bronze · CSV → Parquet  (notebook 01)

| Dataset | Filas | CSV | Parquet snappy | Ratio | Tiempo |
|---|---:|---:|---:|---:|---:|
| MEN | 15,707 | 5 MB | 1.3 MB | 3.8× | 14 s |
| Internet Fijo (MinTIC) | 2,795,052 | 384 MB | 30 MB | 12.8× | 26 s |
| SISBEN Personas (DNP) | 4,465,955 | 937 MB | 63 MB | 14.9× | 95 s |
| ICFES Saber 11 | 7,109,704 | 3,515 MB | 292 MB | 12.0× | 157 s |
| **TOTAL** | **14.4 M** | **4.8 GB** | **386 MB** | **~12×** | **≈ 5 min** |

Espacio HDFS final usado (rep=3): 15.8 GB / 44.8 GB (35 %).

---

## 2. Silver · limpieza y normalización  (notebooks 02–05)

| Notebook | Entrada | Salida | Tiempo |
|---|---:|---:|---:|
| 02 — ICFES | 7,109,704 | **4,500,067** filas (descartó nulls y `PUNT_GLOBAL=0`) | 91 s |
| 03 — Internet | 2,795,052 | 2,795,052 (parseó "8,00" → 8.0, renombró `No DE ACCESOS`) | 98 s |
| 04 — SISBEN | 4,465,955 personas | 1,098 **municipios agregados** (avg de 15 indicadores I1..I15) | 79 s |
| 05 — MEN | 15,707 | 15,707 (slugify de columnas con acentos, "%"→double) | 57 s |

Particionado por `ANO` en datasets temporales para queries más rápidas.

---

## 3. Gold · panel municipal año  (notebook 06)

**Mesa analítica final:** `7,004 filas × 44 columnas`
Grano: `(COD_MPIO, ANO)`. Join LEFT desde ICFES contra Internet + SISBEN (broadcast) + MEN.

Variables clave: `avg_punt_global`, `pct_internet_icfes`, `total_accesos`, `idx_privacion`, `pct_grupo_A`, `COBERTURA_NETA`, `DESERCION`, etc.

---

## 4. EDA · preguntas de negocio  (notebook 07)

Gráficos en `proyecto/evidencia/`:

| Pregunta | Archivo |
|---|---|
| Q1 — Internet vs puntaje (scatter ponderado) | `q1_internet_vs_puntaje.png` |
| Q2 — Pobreza SISBEN vs puntaje (por quintil) | `q2_privacion_vs_puntaje.png` |
| Q4 — Evolución temporal de la brecha (2013–2024) | `q4_evolucion_temporal.png` |
| Matriz de correlación de variables clave | `heatmap_correlaciones.png` |

---

## 5. MLlib supervisado · Random Forest  (notebook 08)

**Objetivo:** predecir `avg_punt_global` municipal a partir de conectividad + pobreza + cobertura educativa.
**Pipeline:** `Imputer(median) → VectorAssembler → StandardScaler → RandomForestRegressor(numTrees=80, maxDepth=10)`.

| Set | RMSE | MAE | R² |
|---|---:|---:|---:|
| Train (5,233) | 11.67 | 9.15 | **0.706** |
| **Test (1,269)** | **13.51** | **10.34** | **0.577** |

Sobre puntaje 0–500, RMSE = 13.5 ⇒ ~2.7 % de error.
Modelo persistido en HDFS: `/usr/proyecto/models/rf_punt_global/`.

---

## 6. MLlib no supervisado · K-Means  (notebook 09)

Tipologías municipales sobre el snapshot del año más reciente (1,098 municipios).
Probamos k ∈ [2, 8] con WCSS y coeficiente de silueta:

| k | WCSS | silueta |
|--:|--:|--:|
| 2 | 4398 | **0.448** ← elegido |
| 3 | 3568 | 0.399 |
| 4 | 3262 | 0.332 |
| 5 | 3047 | 0.324 |
| 8 | 2485 | 0.278 |

Modelo persistido: `/usr/proyecto/models/kmeans_k2/`.
Gráficos: `kmeans_eleccion_k.png`, `kmeans_clusters_scatter.png`.

---

## 7. Evidencia capturada (`proyecto/evidencia/`)

- `spark_master_state.json` — estado del Spark Master con workers, executors, apps.
- `spark_master_home.html` — UI del master como snapshot HTML.
- `spark_apps_summary.txt` — historial de apps ejecutadas (TODAS de esta corrida visibles).
- `hdfs_dfsadmin_report.txt`, `hdfs_layout_proyecto.txt`, `hdfs_sizes_proyecto.txt` — estado HDFS.
- `hdfs_namenode_jmx.json` — métricas JMX del NameNode.
- `inventario_hdfs.txt` — listado completo Bronze + Silver + Gold + Modelos.
- 6 gráficos PNG (EDA + KMeans).

**Para las screenshots reales** (que el profe suele pedir), entrar al cluster por SSH-túnel o desde el navegador del cluster y capturar:
- HDFS NameNode UI: http://10.43.97.164:9870
- Spark Master UI: http://10.43.97.164:8080
- Spark App UI: http://10.43.97.164:4040 (mientras corre una app — relanzar cualquier notebook para verla)

---

## 8. Cómo reproducir / re-ejecutar

```bash
# Desde el Mac
ssh mpde12        # alias en ~/.ssh/config; key auth ya configurada

# En el remoto
bash ~/proyecto_datos/scripts/run_all.sh           # de 00 a 09
# o solo silver→ml:
bash ~/proyecto_datos/scripts/run_silver_to_ml.sh
# o un notebook puntual:
cd ~/proyecto_datos/notebooks_entrega2
jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=2400 06_gold_panel_municipal.ipynb
```

Para regenerar un notebook desde su spec (después de editar):
```bash
python3 ~/proyecto_datos/specs/spec_06_gold_panel.py
```
