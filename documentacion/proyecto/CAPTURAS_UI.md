# Capturas de Spark UI y HDFS UI — Evidencia para la Entrega 2

> Tu Mac tiene **acceso directo** a las UIs del cluster (estás en la red de la Javeriana).
> No necesitas túnel SSH.

---

## URLs

| UI | URL | Disponibilidad |
|---|---|---|
| **HDFS NameNode** | http://10.43.97.164:9870 | siempre |
| **Spark Master** | http://10.43.97.164:8080 | siempre |
| **Spark App (Jobs/Stages/Executors)** | http://10.43.97.164:4040 | **solo mientras corre una app** |
| JupyterLab | http://10.43.97.164:8888 | siempre |

---

## Qué capturar (mínimo) en cada UI

### 1. HDFS NameNode UI — http://10.43.97.164:9870

- **Pestaña "Overview"**: capacidad total, espacio usado, # datanodes vivos.
- **Pestaña "Datanodes"**: los 3 workers (10.43.97.163/.198/.208) vivos.
- **Pestaña "Utilities → Browse the file system"** → navega a `/usr/proyecto/`
  y abre las subcarpetas para mostrar la estructura Bronze/Silver/Gold.

### 2. Spark Master UI — http://10.43.97.164:8080

- Página principal: workers, cores totales (16), memoria total (42 GB).
- Sección **"Completed Applications"**: verás todas las apps `Entrega2-*` ejecutadas.
- Click sobre una app → Jobs, Stages, Executors (historial).

### 3. Spark App UI — http://10.43.97.164:4040 ⚠️ ventana limitada

**Solo está vivo mientras un notebook está ejecutándose.** Captura:

- **Jobs** activos (lista con duración y stages).
- **Stages** con tasks paralelas (verás barras de progreso por executor).
- **Executors** (verás los workers REALES del cluster, no solo el driver).
- **SQL/DataFrame** (si quieres ver el plan físico de una query).

---

## Cómo mantener :4040 vivo

Tienes dos opciones:

### Opción A — pedir al asistente que lance una app

> "lanza una app larga para capturar :4040"

(el asistente corre uno de los notebooks pesados en background; tienes ~3–5 min).

### Opción B — hacerlo tú mismo

1. Entra a http://10.43.97.164:8888 (JupyterLab).
2. Abre cualquier notebook de `proyecto_datos/notebooks_entrega2/` (por ejemplo `08_ml_supervisado_regresion.ipynb`).
3. Ejecuta las primeras celdas hasta que el SparkSession esté creado.
4. Mientras el kernel viva, :4040 responde.

---

## Cómo tomar screenshots en macOS

| Atajo | Acción |
|---|---|
| `Cmd + Shift + 4` | cursor en cruz → seleccionas área → PNG en Escritorio |
| `Cmd + Shift + 4` y luego `Space` | captura una ventana completa |
| `Cmd + Shift + 5` | menú con opciones (incluye grabar video) |

---

## Dónde guardar los PNG

Muévelos a la carpeta del proyecto con nombres descriptivos. Sugerencia:

```
proyecto/evidencia/
├── hdfs_overview.png
├── hdfs_datanodes.png
├── hdfs_browse_proyecto.png
├── spark_master_home.png
├── spark_master_app_history.png
├── spark_app_jobs.png
├── spark_app_stages.png
├── spark_app_executors.png
└── jupyter_notebook_corriendo.png   (opcional)
```

Comando para mover en bloque desde el Escritorio:

```bash
mv ~/Desktop/spark_*.png ~/Desktop/hdfs_*.png \
   /Users/juanbaplo/ProcesamientoDatos/proyecto/evidencia/
```

---

## Si en algún momento ya no se ve nada

```bash
# Verificar que las UIs siguen vivas
curl -s -o /dev/null -w "9870: %{http_code}\n" http://10.43.97.164:9870
curl -s -o /dev/null -w "8080: %{http_code}\n" http://10.43.97.164:8080
curl -s -o /dev/null -w "4040: %{http_code}\n" http://10.43.97.164:4040
```

200/302 = OK. Otra cosa = caída o sin app corriendo.
