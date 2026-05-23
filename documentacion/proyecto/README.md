# Proyecto Entrega 2 — Procesamiento de Datos a Gran Escala
## Brecha digital y resultados Saber 11 en Colombia
**Grupo:** REST pAPIs

Pipeline Big Data **Hadoop HDFS + Apache Spark + MLlib** que cruza ICFES Saber 11,
acceso a internet (MinTIC), pobreza multidimensional (DNP SISBEN) y educación
municipal (MEN) para analizar la relación entre **brecha digital** y
**rendimiento académico** a nivel municipal en Colombia.

---

## Infraestructura

| Componente | Versión / Endpoint |
|---|---|
| Hadoop HDFS | 3.3.6 — `hdfs://10.43.97.164:9000` |
| Spark standalone | 3.5.2 — `spark://spark-master:7077` (4 workers · 16 cores · 42 GB RAM) |
| Python / PySpark | 3.9 / 3.5.2 |
| JupyterLab | 4.5.6 — `http://10.43.97.164:8888` |
| HDFS NameNode UI | `http://10.43.97.164:9870` |
| Spark Master UI | `http://10.43.97.164:8080` |
| Spark App UI | `http://10.43.97.164:4040` (puerto dinámico cuando corre una app) |

---

## Arquitectura de datos en HDFS

```
/usr/proyecto/
├── bronze/              ← CSVs crudos (renombrados a ASCII para evitar bugs de encoding)
│   ├── icfes/Resultados_unicos_Saber_11_20260519.csv        (3.5 GB, 7.1M filas)
│   ├── internet/Internet_Fijo_Accesos_..._20260407.csv      (384 MB, 2.8M filas)
│   ├── sisben/DNP_Sisben_Personas_20260519.csv              (937 MB, 4.5M filas)
│   └── men/MEN_Estadisticas_Educacion_Municipio_20260519.csv (5 MB, 16K filas)
├── bronze_parquet/      ← versiones Parquet snappy de los CSV (~12× compresión)
├── silver/              ← limpio, columnas tipadas, geo normalizado
│   ├── icfes/           (filtra PUNT_GLOBAL>0, binariza FAMI_TIENEINTERNET, etc.)
│   ├── internet/        (parsea "8,00" → 8.0, renombra "No DE ACCESOS")
│   ├── sisben_municipal/ (agrega 4.5M personas → 1 fila por municipio)
│   └── men/             (parsea "56.11%" → 0.5611, normaliza nombres con acentos)
├── gold/
│   └── panel_municipal/ ← mesa analítica (COD_MPIO × AÑO) cruzando las 4 fuentes
└── models/
    ├── rf_punt_global/  ← Random Forest persistido (Entrega 2 — supervisado)
    └── kmeans_kN/       ← K-Means persistido (Entrega 2 — no supervisado)
```

---

## Notebooks (orden de ejecución)

| # | Notebook | Qué hace | Salida |
|---|---|---|---|
| 00 | `00_setup_verificacion` | Conecta SparkSession contra el cluster, verifica HDFS, lee un CSV pequeño | — |
| 01 | `01_bronze_csv_a_parquet` | Convierte los 4 CSVs Bronze → Parquet snappy | `/usr/proyecto/bronze_parquet/...` |
| 02 | `02_silver_icfes` | Limpia ICFES: filtros, casting, normalización geo, binarias | `/usr/proyecto/silver/icfes/` (partitioned by ANO) |
| 03 | `03_silver_internet` | Parsea velocidades coma-decimal, normaliza códigos DANE | `/usr/proyecto/silver/internet/` (partitioned by ANO) |
| 04 | `04_silver_sisben` | Agrega 4.5M personas → métricas por municipio | `/usr/proyecto/silver/sisben_municipal/` |
| 05 | `05_silver_men` | Parsea "%" → double, slugify de columnas | `/usr/proyecto/silver/men/` (partitioned by ANO) |
| 06 | `06_gold_panel_municipal` | LEFT JOIN ICFES⋈Internet⋈SISBEN⋈MEN por (mpio, año) | `/usr/proyecto/gold/panel_municipal/` |
| 07 | `07_eda_y_preguntas_negocio` | Responde 5 preguntas de negocio con gráficos | `evidencia/q1..q4*.png`, `heatmap_*.png` |
| 08 | `08_ml_supervisado_rf` | RandomForestRegressor predice `avg_punt_global` municipal | `/usr/proyecto/models/rf_punt_global/` |
| 09 | `09_ml_no_supervisado_kmeans` | K-Means de tipologías municipales (codo + silueta) | `/usr/proyecto/models/kmeans_kN/` |

**Ejecutar todos en orden:**
```bash
bash ~/proyecto_datos/scripts/run_all.sh
```
o uno a uno con:
```bash
cd ~/proyecto_datos/notebooks_entrega2 && \
jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.timeout=2400 02_silver_icfes.ipynb
```

---

## Estructura del repo local (Mac)

```
proyecto/
├── README.md                       (este archivo)
├── scripts/
│   ├── build_notebook.py           (helper nbformat)
│   ├── common_spark.py             (SparkSession + path constants)
│   └── run_all.sh                  (cadena de ejecución remota)
├── specs/                          (1 archivo .py por notebook — fuente de verdad)
│   ├── spec_00_setup.py
│   ├── spec_01_bronze.py
│   ├── ...
│   └── spec_09_ml_kmeans.py
├── notebooks_entrega2/             (copia local de los .ipynb ejecutados)
└── evidencia/                      (capturas Spark UI, HDFS UI, gráficos)
```

**Regenerar notebooks desde specs:**
```bash
python3 specs/spec_06_gold_panel.py    # escribe el .ipynb en el remoto
```

---

## Decisiones técnicas

- **Bronze inmutable + Parquet snappy.** Compresión ~12× (4.8 GB CSV → 386 MB Parquet).
- **HDFS path = `/usr/proyecto/`** (no `/user/`) por convención del cluster preexistente.
- **Archivos HDFS renombrados a ASCII** para evitar el bug NFC vs NFD de macOS al pasar paths con acentos por SSH/heredoc.
- **SISBEN y MEN tenían duplicados byte-idénticos** (md5 igual) — se eliminó uno; los locales se conservan como respaldo.
- **Silver particionado por `ANO`** en datasets temporales (ICFES, Internet, MEN) para queries más rápidas.
- **SISBEN agregado a municipio** (no por persona) — el grano fino no ayuda al join con resultados Saber 11.
- **Joins en Gold**: LEFT JOIN partiendo de ICFES (el outcome). SISBEN se broadcast (chico).
- **ML target = `avg_punt_global`** a nivel municipal (no estudiante) para tener features contextuales ricos.

---

## Preguntas de negocio respondidas en `07_eda`

1. ¿Correlación entre conectividad y Saber 11? (scatter ponderado + matriz de correlación)
2. ¿Pobreza (SISBEN) vs rendimiento? (por quintil de privación)
3. ¿Brecha rural / urbano?
4. ¿La brecha se cierra con el tiempo? (evolución 2013–2024)
5. Top y bottom 10 municipios — perfil de contexto.
