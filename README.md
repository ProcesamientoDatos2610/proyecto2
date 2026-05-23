# Brecha digital y resultados Saber 11 en Colombia

> Proyecto final de la asignatura **Procesamiento de Datos a Gran Escala**
> Pontificia Universidad Javeriana — Facultad de Ingeniería
> Departamento de Ingeniería de Sistemas
> Profesor: Fabián Pallares
> Grupo: **REST pAPIs**

Pipeline de Big Data implementado sobre un cluster propio **Apache Hadoop 3.3.6** + **Apache Spark 3.5.2** para analizar la relación entre acceso a internet, condiciones socioeconómicas (Sisbén), cobertura educativa (MEN) y los resultados de las pruebas Saber 11 (ICFES) a nivel municipal en Colombia.

El proyecto sigue la metodología CRISP-DM, integra cuatro fuentes de datos abiertos del Estado colombiano, procesa más de 14 millones de registros distribuidamente y entrena cuatro modelos de aprendizaje de máquina con Spark MLlib (Regresión Lineal, Árbol de Decisión, K-Means y un MLP de red neuronal como bono).

---

## Tabla de contenidos

1. [Equipo](#equipo)
2. [Contexto y problema de negocio](#contexto-y-problema-de-negocio)
3. [Requerimientos del enunciado](#requerimientos-del-enunciado)
4. [Estructura del repositorio](#estructura-del-repositorio)
5. [Infraestructura del cluster](#infraestructura-del-cluster)
6. [Arquitectura de datos (Medallion)](#arquitectura-de-datos-medallion)
7. [Notebooks del pipeline](#notebooks-del-pipeline)
8. [Hallazgos clave](#hallazgos-clave)
9. [Modelos entrenados](#modelos-entrenados)
10. [Las 8 preguntas de negocio](#las-8-preguntas-de-negocio)
11. [Bonos ejecutados](#bonos-ejecutados)
12. [Cómo reproducir el proyecto](#cómo-reproducir-el-proyecto)
13. [Limitaciones y trabajo futuro](#limitaciones-y-trabajo-futuro)
14. [Fuentes de datos](#fuentes-de-datos)

---

## Equipo

| Integrante |
|---|
| Juan Camilo Carvajal Rodríguez |
| Juan Pablo Cañón Contreras |
| Juan David Rincón Poveda |
| Tatiana Vivas Restrepo |

---

## Contexto y problema de negocio

El equipo actúa como consultor de analítica contratado por el **Ministerio de Educación Nacional** para construir un plan de acción que mejore los resultados de la prueba Saber 11 en municipios vulnerables, particularmente analizando el rol de la **brecha digital** como factor explicativo.

Los resultados de Saber 11 reflejan desigualdades estructurales del país: hay una diferencia de hasta 60 puntos entre los departamentos con mejor y peor desempeño, una brecha urbano-rural cercana a los 26 puntos y un Índice de Pobreza Multidimensional rural que duplica al urbano. En este contexto, la conectividad digital se ha planteado como una palanca de política pública con potencial para reducir esas brechas, pero su efectividad real necesita evidencia cuantitativa.

El proyecto busca generar esa evidencia: cuantificar el peso de la conectividad sobre el rendimiento académico, contrastarlo contra otros factores socioeconómicos, identificar tipologías de municipios para focalizar la inversión y construir modelos predictivos que apoyen la toma de decisiones.

---

## Requerimientos del enunciado

El proyecto se desarrolla en dos entregas (cada una calificada con 70% documento + 30% sustentación oral de 15 minutos).

### Entrega 1 — Entendimiento del negocio y de los datos

1. **Entendimiento del negocio**: contexto territorial, indicadores macroeconómicos, definición del problema y del objetivo de consultoría.
2. **Selección de datos**: justificación de las fuentes elegidas en relación con el objetivo.
3. **Colección y descripción de datos**: carga al cluster Spark, tipos de datos, significado de cada atributo, descripción general de cada dataset.
4. **Exploración de los datos**: estadísticos descriptivos, gráficas, tablas agregadas (mínimo 8 elementos de análisis).
5. **Reporte de calidad de datos**: conteo de valores faltantes y técnicas propuestas de tratamiento.
6. **Planteamiento de preguntas de negocio**: mínimo 8 preguntas a responder en la Entrega 2.
7. **Filtros, limpieza y transformación inicial**: avance preliminar de la limpieza.

**Bonos de Entrega 1** (+0.25 cada uno): web scraping de la tabla de población y consumo de la API del clima.

### Entrega 2 — Preparación, modelado y resultados

1. **Filtros y transformaciones**: mínimo 2 filtros y 3 transformaciones por dataset con justificación.
2. **Respuesta a las preguntas de negocio**: tablas y visuales que respondan las 8 preguntas planteadas en la Entrega 1.
3. **Selección de técnicas de aprendizaje de máquina**: 1 supervisada y 1 no supervisada con justificación.
4. **Preparación de datos para modelado**: análisis de correlación, eliminación de variables redundantes, normalización y selección por criterio de negocio.
5. **Aplicación de las técnicas con Spark MLlib** sobre el cluster.
6. **Métricas de evaluación** con pruebas de diferentes parámetros.

**Bonos de Entrega 2** (+0.25 cada uno): modelo de aprendizaje profundo (redes neuronales) y solución de despliegue.

**Bonos adicionales** (+0.05 y +0.1 sobre nota final): repositorio con README y citas mediante herramienta bibliográfica (Zotero/Mendeley).

---

## Estructura del repositorio

Este repositorio contiene los **10 notebooks Jupyter** que materializan el pipeline completo, ejecutados sobre el cluster con sus outputs incluidos.

```
proyecto2/
├── README.md                            (este archivo)
├── 00_setup_verificacion.ipynb          Conexión Spark + HDFS, sanity check
├── 01_bronze_csv_a_parquet.ipynb        Ingesta de 4 CSVs y conversión a Parquet
├── 02_silver_icfes.ipynb                Limpieza ICFES (4.5M filas tras filtros)
├── 03_silver_internet.ipynb             Limpieza Internet Fijo
├── 04_silver_sisben.ipynb               Limpieza Sisbén + agregación a municipio
├── 05_silver_men.ipynb                  Limpieza estadísticas educativas MEN
├── 06_gold_panel_municipal.ipynb        Joins multi-fuente → vista minable
├── 07_eda_y_preguntas_negocio.ipynb     EDA + respuesta a las 8 preguntas
├── 08_ml_supervisado_regresion.ipynb    Regresión Lineal + Decision Tree con grid
└── 09_ml_no_supervisado_kmeans.ipynb    K-Means con método del codo + silueta
```

### Otros entregables (fuera de este repositorio)

| Componente | Ubicación |
|---|---|
| Documento PDF (62 páginas) | Entregado por separado al profesor |
| Código fuente LaTeX | Carpeta local del equipo |
| Notebook MLP bono (red neuronal) | `10_bono_mlp_estudiante.ipynb` en el cluster |
| Datos crudos y procesados | HDFS del cluster (`/usr/proyecto/`) |
| Modelos entrenados | HDFS (`/usr/proyecto/models/`) |
| Figuras EDA generadas | Embebidas en el documento PDF |

---

## Infraestructura del cluster

| Componente | Valor |
|---|---|
| Host principal | `MPDE12` (Rocky Linux 9) |
| NameNode | `10.43.97.164` |
| DataNodes / Workers | 3 (`10.43.97.163`, `10.43.97.198`, `10.43.97.208`) |
| Cores totales | 16 (4 por worker) |
| RAM total | ~42 GB (10.6 GB por worker) |
| Hadoop | 3.3.6 (HDFS + YARN, replicación 3) |
| Spark | 3.5.2 standalone (`spark://spark-master:7077`) |
| Python / PySpark | 3.9.25 / 3.5.2 |
| Jupyter | JupyterLab 4.5.6 en puerto `:8888` |

### URLs de monitoreo del cluster (vía túnel SSH desde la red Javeriana)

```
http://10.43.97.164:9870  -> HDFS NameNode UI
http://10.43.97.164:8080  -> Spark Master UI
http://10.43.97.164:4040  -> Spark Application UI (solo cuando una app corre)
http://10.43.97.164:8888  -> JupyterLab
```

---

## Arquitectura de datos (Medallion)

Se implementó el patrón medallion **Bronze → Silver → Gold** sobre HDFS:

```
/usr/proyecto/
├── bronze/             CSVs crudos descargados de datos.gov.co
├── bronze_parquet/     versión Parquet snappy (compresión ~12x)
├── silver/             datos limpios, tipados, geo normalizado
│   ├── icfes/                  (particionado por año)
│   ├── internet/               (particionado por año)
│   ├── sisben_municipal/       (1 fila por municipio)
│   └── men/                    (particionado por año)
├── gold/
│   └── panel_municipal/        Vista minable: 7,004 filas × 44 columnas
└── models/
    ├── lr_punt_global/         Regresión Lineal
    ├── dt_punt_global/         Decision Tree Regressor
    ├── kmeans_k2/              K-Means con k=2
    └── mlp_rango_estudiante/   MLP red neuronal (bono)
```

### Compresión obtenida

| Dataset | CSV original | Parquet | Ratio |
|---|---:|---:|---:|
| ICFES Saber 11 | 3,515 MB | 292 MB | 12.0x |
| Internet Fijo | 384 MB | 30 MB | 12.8x |
| Sisbén Personas | 937 MB | 63 MB | 14.9x |
| MEN Educación | 5 MB | 1.3 MB | 3.8x |
| **Total** | **4.8 GB** | **386 MB** | **~12x** |

---

## Notebooks del pipeline

### 00. Setup y verificación

Conexión inicial al cluster, validación de la conectividad con HDFS, prueba de lectura sobre un dataset pequeño (MEN). Permite confirmar que el SparkSession se crea correctamente apuntando al master del cluster y no a un local.

### 01. Bronze: CSV a Parquet

Conversión de los 4 CSVs crudos a Parquet con compresión Snappy. Las claves del proceso son: lectura con `multiLine=True` y `escape="\""` para manejar campos multilínea, escritura con `mode("overwrite")` para idempotencia y verificación posterior del conteo de filas para garantizar integridad.

### 02. Silver ICFES

Filtros aplicados:
- F1: `PUNT_GLOBAL IS NOT NULL`
- F2: `PUNT_GLOBAL > 0` (descarta cero, indica registro no presentado)
- F3: `PUNT_GLOBAL < 500` (máximo teórico)

Transformaciones aplicadas: cast de tipos, parseo de puntajes por área con coma decimal, binarización de `FAMI_TIENEINTERNET` y `FAMI_TIENECOMPUTADOR`, normalización geográfica (UPPER + sin acentos), zero-padding de códigos DANE, derivación de `ANO` desde `PERIODO` y banding del puntaje global en `RANGO_PUNT_GLOBAL` (BAJO/MEDIO/ALTO).

Resultado: **4,500,067 filas** (de 7,109,704 iniciales — 37% descartado).

### 03. Silver Internet

Filtros aplicados:
- F1: `NUM_ACCESOS IS NOT NULL AND NUM_ACCESOS >= 0`
- F2: `VELOCIDAD_BAJADA > 0`

Transformaciones aplicadas: renombrado de `AÑO` y `No DE ACCESOS`, cast de tipos, parseo de velocidades con coma decimal, zero-padding de códigos DANE, normalización de nombres categóricos.

Resultado: **2,792,934 filas** (de 2,795,052 — 0.08% descartado).

### 04. Silver Sisbén

Filtros aplicados (a nivel persona, antes de agregar):
- F1: `cod_mpio IS NOT NULL`
- F2: `Grupo IN ('A','B','C','D')`

Transformaciones aplicadas: tipificación de los 15 indicadores `I1` a `I15`, zero-padding del código de municipio, agregación masiva persona → municipio con `groupBy` (n_personas, n_hogares, fex_total, distribución por grupo y zona, promedios de los 15 indicadores), derivación de `idx_privacion`.

Resultado: **1,099 municipios** (de 4,465,955 personas iniciales).

### 05. Silver MEN

Filtros aplicados:
- F1: `COD_MPIO AND ANO no nulos`
- F2: `POBLACION_5_16 > 0`

Transformaciones aplicadas: slugify de las 41 columnas (eliminación de acentos y caracteres especiales), cast de tipos, parseo de porcentajes (`"56.11%"` → `0.5611`), zero-padding de códigos DANE, particionado por año.

Resultado: **15,700 filas** (de 15,707 — 7 filas descartadas).

### 06. Gold: panel municipal

Construye la **vista minable** uniendo las cuatro fuentes Silver mediante joins por `(COD_MPIO, ANO)`:
- ICFES Silver agregado a municipio-año (puntajes promedio, % internet, % computador, % rural)
- Internet Silver agregado (total accesos, n proveedores, velocidad promedio)
- MEN Silver (cobertura, deserción, aprobación)
- Sisbén Silver (broadcast join, sin dimensión temporal)

Resultado: tabla de **7,004 filas × 44 columnas**, base directa de los análisis de la Entrega 2.

### 07. EDA y respuesta a las 8 preguntas de negocio

Análisis exploratorio sobre el panel Gold respondiendo cada una de las 8 preguntas planteadas en la Entrega 1, con tablas agregadas en Spark, correlaciones de Pearson calculadas con `df.stat.corr()` y gráficos matplotlib. Análisis específicos:

- Correlación general internet-puntaje
- Cross-tab zona × cuartil de internet
- Correlación pobreza-puntaje DENTRO de cada cuartil de internet (control)
- Comparativa alta vs baja penetración (percentiles 80 y 20)
- Ranking de correlaciones de Pearson de 12 features
- Correlación cobertura-conectividad
- Análisis a nivel estudiante por área evaluada
- Identificación de municipios anómalos

### 08. ML Supervisado: Regresión

Pipeline de Spark MLlib: `Imputer (mediana) → VectorAssembler → StandardScaler → Modelo`.

**Modelos entrenados** (ambos cubiertos en clase):
1. `LinearRegression` con grid de 12 combinaciones (`regParam` × `elasticNetParam`)
2. `DecisionTreeRegressor` con grid de 15 combinaciones (`maxDepth` × `minInstancesPerNode`)

**Análisis de correlación**: matriz de Pearson entre las 12 features, identificación de pares con `|r| > 0.85` (ninguno superó el umbral).

**Mejores modelos**: LR (test R² = 0.4070) y DT (test R² = 0.5214, RMSE = 14.62 puntos).

### 09. ML No Supervisado: K-Means

Pipeline análogo al supervisado pero terminando en `KMeans`. Selección de `k` mediante método del codo (WCSS) + coeficiente de silueta sobre k ∈ [2, 8].

Resultado: **k = 2** con silueta 0.4493. Cluster 0 (603 municipios) caracteriza alta vulnerabilidad; Cluster 1 (495 municipios) caracteriza baja vulnerabilidad.

---

## Hallazgos clave

### Volumetría

| Concepto | Valor |
|---|---|
| Datasets integrados | 4 fuentes oficiales |
| Filas crudas totales | 14.4 millones |
| Filas en vista minable Gold | 7,004 (municipio-año) |
| Municipios cubiertos | ~1,098 (de 1,124 del país) |
| Cobertura temporal | 2011-2024 (Sisbén sin dimensión temporal) |
| Variables finales para ML | 12 (tras análisis de correlación) |

### Estadísticas centrales

- Correlación internet-puntaje a nivel municipal: **r = +0.41**
- Diferencia ALTA vs BAJA penetración de internet: **+22.1 puntos** Saber 11
- Correlación privación-puntaje (sin controlar): r = -0.52
- Correlación privación-puntaje DENTRO de cada cuartil de internet: r entre -0.40 y -0.52
- Efecto del computador en casa por área (con vs sin): +5.7 a +8.3 puntos
- Variable más explicativa (Decision Tree): `idx_privacion` (importancia 48.3%)
- Segunda más explicativa: `pct_internet_icfes` (importancia 23.3%)

### Conclusión cuantitativa principal

La pobreza multidimensional incide en el rendimiento académico **independientemente del nivel de conectividad** del municipio. Esto significa que la conectividad por sí sola no compensa los efectos de la vulnerabilidad socioeconómica: una política de cierre de brecha digital debe ir acompañada de intervenciones sociales integrales para producir mejoras en Saber 11.

---

## Modelos entrenados

| Modelo | Tipo | Grano | Métrica principal | Resultado | Persistido en HDFS |
|---|---|---|---|---|---|
| LinearRegression | Supervisado (regresión) | Municipio-año | Test R² | 0.4070 | `/usr/proyecto/models/lr_punt_global` |
| DecisionTreeRegressor | Supervisado (regresión) | Municipio-año | Test R² | 0.5214 | `/usr/proyecto/models/dt_punt_global` |
| K-Means | No supervisado | Municipio (snapshot) | Silueta | 0.4493 (k=2) | `/usr/proyecto/models/kmeans_k2` |
| MultilayerPerceptron (bono) | Supervisado (clasificación) | Estudiante | Test Accuracy | 0.6899 | `/usr/proyecto/models/mlp_rango_estudiante` |

Todos los modelos se persisten con el método `.write().overwrite().save(...)` de Spark MLlib y se pueden recargar en cualquier sesión futura con la clase correspondiente (`LinearRegressionModel.load(...)`, etc.) sin necesidad de reentrenar.

---

## Las 8 preguntas de negocio

Cada pregunta se responde formalmente en el notebook `07_eda_y_preguntas_negocio.ipynb` y en la sección 7 del documento PDF.

1. **¿Cuál es la relación entre el acceso a internet en el hogar y el desempeño Saber 11 a nivel municipal?**
   Respuesta: r = +0.41 (Pearson). Diferencia de 19 puntos entre cuartil inferior y superior.

2. **¿Cómo varía el rendimiento entre zonas rurales y urbanas en función del nivel de conectividad?**
   Respuesta: La brecha urbano-rural persiste, pero en el cuartil superior de internet los rurales (258) igualan a los urbanos (251).

3. **¿En qué medida la pobreza multidimensional incide en los resultados, incluso en territorios con niveles similares de internet?**
   Respuesta: La correlación pobreza-puntaje se mantiene negativa (entre -0.40 y -0.52) dentro de los cuatro cuartiles de internet.

4. **¿Qué diferencias se observan entre municipios con alta vs baja penetración de internet?**
   Respuesta: Diferencia neta de +22.1 puntos a favor del grupo de alta penetración.

5. **¿Qué variables socioeconómicas presentan mayor capacidad explicativa frente al acceso a internet?**
   Respuesta: `pct_grupo_D` (r = +0.42) y `idx_privacion` (r = -0.38) son tan o más explicativas que `pct_internet_icfes` (r = +0.41).

6. **¿Cuál es la relación entre la cobertura educativa y la conectividad?**
   Respuesta: r(cobertura_neta, internet) = +0.24, correlación moderada positiva.

7. **¿Cómo influye el acceso a computador en el hogar en los resultados por área evaluada?**
   Respuesta: Efecto positivo en TODAS las áreas, máximo en inglés (+8.25 puntos).

8. **¿Qué municipios presentan alta conectividad pero bajos resultados académicos?**
   Respuesta: 10 municipios anómalos identificados, concentrados en Antioquia (5 de 10).

---

## Bonos ejecutados

### Bonos de Entrega 1

- **Web scraping de tabla de población** (Wikipedia / DANE) integrado al análisis territorial.
- **API del clima** consumida para 12 ciudades colombianas con visualizaciones de temperatura, humedad y probabilidad de lluvia.
- Ambos detallados en la sección de bonos del documento PDF.

### Bonos de Entrega 2

- **Modelo de aprendizaje profundo (MLP)**: red neuronal feed-forward con arquitectura `[76, 32, 3]` clasificando estudiantes en BAJO/MEDIO/ALTO. Entrenada distribuidamente sobre los 4.5M estudiantes del Silver ICFES utilizando `MultilayerPerceptronClassifier` de Spark MLlib. Test accuracy de 68.99%.

### Bonos adicionales

- **Repositorio Git con README** (este documento).
- **Herramienta bibliográfica**: las citas del documento PDF se gestionaron con Zotero, generando referencias APA consistentes.

---

## Cómo reproducir el proyecto

### Requisitos

- Acceso al cluster MPDE12 (red Javeriana o VPN) con credenciales del usuario `estudiante`.
- SSH configurado (`ssh estudiante@10.43.97.164`).

### Configuración inicial

1. **Clonar el repositorio** en el cluster o en local.
2. **Subir los notebooks a JupyterLab** del cluster (puerto 8888) si se ejecutan desde allí.
3. **Verificar que los datos Bronze ya estén en HDFS** en `/usr/proyecto/bronze/`. Si no, cargarlos desde los CSVs originales con `hdfs dfs -put`.

### Orden de ejecución

Los notebooks deben ejecutarse en orden, ya que cada uno depende de los outputs persistidos del anterior:

```
00_setup_verificacion       (validación)
   -> 01_bronze_csv_a_parquet   (escribe en bronze_parquet/)
      -> 02_silver_icfes        (lee bronze_parquet, escribe silver/)
      -> 03_silver_internet     (idem)
      -> 04_silver_sisben       (idem)
      -> 05_silver_men          (idem)
         -> 06_gold_panel_municipal  (lee silver/, escribe gold/)
            -> 07_eda_y_preguntas_negocio  (lee gold/)
            -> 08_ml_supervisado_regresion (lee gold/, escribe models/)
            -> 09_ml_no_supervisado_kmeans (lee gold/, escribe models/)
```

Cada notebook puede ejecutarse con JupyterLab interactivamente o desde línea de comandos:

```bash
jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=2400 \
    <nombre_notebook>.ipynb
```

### Acceso a los modelos persistidos

Los modelos entrenados se pueden recargar en cualquier sesión PySpark con:

```python
from pyspark.ml.regression import LinearRegressionModel, DecisionTreeRegressionModel
from pyspark.ml.clustering import KMeansModel

lr = LinearRegressionModel.load("hdfs://10.43.97.164:9000/usr/proyecto/models/lr_punt_global")
dt = DecisionTreeRegressionModel.load("hdfs://10.43.97.164:9000/usr/proyecto/models/dt_punt_global")
km = KMeansModel.load("hdfs://10.43.97.164:9000/usr/proyecto/models/kmeans_k2")
```

---

## Limitaciones y trabajo futuro

### Limitaciones reconocidas del análisis

- La unidad de análisis principal es **municipio-año**, lo que oculta heterogeneidad intra-municipal entre colegios o entre veredas.
- El dataset MEN ofrece cobertura cuantitativa pero **no captura calidad educativa** propiamente dicha (ratio docente-estudiante, formación docente, infraestructura).
- El análisis es **correlacional**, no causal. No se aplican técnicas de inferencia causal (diff-in-diff, instrumentos) para aislar el efecto de la conectividad.
- El periodo cubierto no diferencia explícitamente el **shock de la pandemia COVID-19** sobre los resultados.
- El MLP a nivel estudiante muestra **desbalance de clases** que afecta el recall sobre las minorías ALTO y BAJO.

### Trabajo futuro propuesto

1. Incorporar datos de **calidad docente** y **infraestructura escolar** del MEN.
2. Modelar a nivel **estudiante individual** con técnicas que mitiguen el desbalance (SMOTE, class weights).
3. Construir un análisis **longitudinal** que cuantifique el efecto de políticas implementadas en el tiempo.
4. **Despliegue del modelo** como tablero de gestión territorial para el Ministerio (bono no ejecutado en esta entrega).

---

## Fuentes de datos

Todos los datasets utilizados son públicos y provienen del portal de datos abiertos del gobierno colombiano:

| Dataset | URL |
|---|---|
| ICFES Saber 11 | https://www.datos.gov.co/Educaci-n/Resultados-nicos-Saber-11/kgxf-xxbe |
| Internet Fijo (MinTIC) | https://www.datos.gov.co/Ciencia-Tecnolog-a-e-Innovaci-n/Internet-Fijo-Accesos-por-tecnolog-a-y-segmento/n48w-gutb |
| Sisbén Personas (DNP) | https://www.datos.gov.co/Estad-sticas-Nacionales/DNP-Sisb-n-Personas/hq2v-5umk |
| MEN Estadísticas Educación Municipal | https://www.datos.gov.co/Educaci-n/MEN_ESTADISTICAS_EN_EDUCACION_EN_PREESCOLAR-B-SICA/nudc-7mev |

---

## Licencia y créditos

Proyecto académico de la Pontificia Universidad Javeriana. Los datos son públicos del Estado colombiano. Cualquier reutilización del código debe citar al grupo REST pAPIs y a la asignatura Procesamiento de Datos a Gran Escala.

Para preguntas sobre el proyecto, contactar al equipo a través del repositorio de GitHub.
