"""Generates 01_bronze_csv_a_parquet.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/01_bronze_csv_a_parquet.ipynb"

CSV_OPTS = '''  .option("header", True)
  .option("encoding", "UTF-8")
  .option("multiLine", True)
  .option("quote", '"')
  .option("escape", '"')'''

cells = [
    md("""
# 01 — Bronze: CSV → Parquet

Convertimos los 4 archivos CSV de la capa Bronze a **Parquet con compresión Snappy**.

**¿Por qué Parquet?**
- **Columnar:** lee solo las columnas que necesitas (clave en ICFES con 51 columnas).
- **Splittable + comprimido:** ~3-5× menos espacio que CSV en disco; ~5-10× más rápido de leer en Spark.
- **Schema embebido:** no tenemos que reinferir cada vez.

**Estrategia:**
- Mantener todo como `string` por ahora (la limpieza/casting va en Silver).
- Sobrescribir si ya existe (modo `overwrite`).
- Verificar conteo de filas antes vs después.
"""),

    md("## 1. Setup"),
    code("""
import sys, time
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P, HDFS_NAMENODE
from pyspark.sql import functions as F

spark = get_spark(
    "Entrega2-Bronze",
    executor_memory="4g",
    driver_memory="2g",
    cores=2,
)
sc = spark.sparkContext
print("App ID :", sc.applicationId)
print("Spark UI (sustituye `spark-master` por 10.43.97.164):", sc.uiWebUrl)
"""),

    code("""
# Helper para tamaño de cualquier ruta en HDFS
URI = sc._jvm.java.net.URI
Path = sc._jvm.org.apache.hadoop.fs.Path
FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI(HDFS_NAMENODE), sc._jsc.hadoopConfiguration())

def hdfs_size_mb(path):
    p = Path(path)
    if not fs.exists(p):
        return None
    return fs.getContentSummary(p).getLength() / 1024 / 1024

def hdfs_ls(path, max_items=5):
    p = Path(path)
    if not fs.exists(p):
        return []
    return [(f.getPath().getName(), f.getLen()) for f in fs.listStatus(p)[:max_items]]

def ingest(name, csv_path, out_path):
    print(f"\\n========= {name} =========")
    print(f"  src: {csv_path}")
    print(f"  dst: {out_path}")
    t0 = time.time()
    df = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"hdfs://10.43.97.164:9000{csv_path}")
    )
    cols = len(df.columns)
    print(f"  columns      : {cols}")
    # Sobrescribimos siempre — bronze parquet es idempotente
    (df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"hdfs://10.43.97.164:9000{out_path}"))
    dur = time.time() - t0
    df_pq = spark.read.parquet(f"hdfs://10.43.97.164:9000{out_path}")
    n = df_pq.count()
    src_mb = hdfs_size_mb(csv_path)
    dst_mb = hdfs_size_mb(out_path)
    ratio = (src_mb or 0) / (dst_mb or 1)
    print(f"  rows escritas: {n:,}")
    print(f"  CSV    size  : {src_mb:>9.1f} MB")
    print(f"  Parquet size : {dst_mb:>9.1f} MB  ({ratio:.1f}x menor)")
    print(f"  tiempo total : {dur:.1f}s")
    return {"name": name, "rows": n, "csv_mb": src_mb, "pq_mb": dst_mb, "secs": dur}
"""),

    md("## 2. MEN (5 MB, calentamiento)"),
    code("""
res_men = ingest("MEN", P.BRONZE_CSV_MEN, P.BRONZE_PQ_MEN)
"""),

    md("## 3. Internet Fijo (385 MB)"),
    code("""
res_internet = ingest("INTERNET", P.BRONZE_CSV_INTERNET, P.BRONZE_PQ_INTERNET)
"""),

    md("## 4. SISBEN Personas (937 MB, 4.5M filas)"),
    code("""
res_sisben = ingest("SISBEN", P.BRONZE_CSV_SISBEN, P.BRONZE_PQ_SISBEN)
"""),

    md("## 5. ICFES Saber 11 (3.5 GB, 7.1M filas) — el grande"),
    code("""
res_icfes = ingest("ICFES", P.BRONZE_CSV_ICFES, P.BRONZE_PQ_ICFES)
"""),

    md("## 6. Resumen consolidado"),
    code("""
import pandas as pd
resumen = pd.DataFrame([res_men, res_internet, res_sisben, res_icfes])
resumen["ratio_compresion"] = (resumen["csv_mb"] / resumen["pq_mb"]).round(2)
resumen[["name","rows","csv_mb","pq_mb","ratio_compresion","secs"]]
"""),

    md("## 7. Inventario Bronze Parquet en HDFS"),
    code("""
for d in [P.BRONZE_PQ_MEN, P.BRONZE_PQ_INTERNET, P.BRONZE_PQ_SISBEN, P.BRONZE_PQ_ICFES]:
    print(f"\\n=== {d} ===")
    items = hdfs_ls(d, max_items=20)
    print(f"  archivos: {len(items)}")
    total = sum(sz for _, sz in items)
    print(f"  total: {total/1e6:.1f} MB")
    for name, sz in items[:5]:
        print(f"    - {name:<60s} {sz/1e6:>7.2f} MB")
"""),

    md("""
## 8. Conclusión

Si el resumen muestra row counts > 0 para los 4 datasets y un ratio de compresión sano (~3-7×),
Bronze está listo.

**Siguiente paso:** `02_silver_icfes.ipynb` — limpieza, normalización y tipado de ICFES.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
