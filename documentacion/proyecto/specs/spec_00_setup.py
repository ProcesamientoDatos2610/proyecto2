"""Generates 00_setup_verificacion.ipynb"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/scripts")
from build_notebook import md, code, build

NB_PATH = "/home/estudiante/proyecto_datos/notebooks_entrega2/00_setup_verificacion.ipynb"

cells = [
    md("""
# 00 — Setup y verificación del entorno

**Proyecto:** Brecha digital y resultados Saber 11 — Grupo *REST pAPIs*
**Entrega 2:** Pipeline Bronze/Silver/Gold + MLlib sobre cluster Hadoop + Spark

Este notebook verifica que el SparkSession se conecta al cluster, que HDFS responde
y que se pueden leer los datos crudos. Es el "hello world" antes de la ingestión real.
"""),

    md("## 1. Imports y SparkSession"),
    code("""
import sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P, SPARK_MASTER, HDFS_NAMENODE
from pyspark.sql import functions as F

spark = get_spark("Entrega2-Setup")
sc = spark.sparkContext
print("Spark version :", spark.version)
print("Master        :", sc.master)
print("App name      :", sc.appName)
print("App ID        :", sc.applicationId)
print("Spark UI      :", sc.uiWebUrl)
print("Default FS    :", spark._jsc.hadoopConfiguration().get("fs.defaultFS"))
"""),

    md("## 2. Recursos del cluster (workers vivos)"),
    code("""
# StatusTracker en PySpark no expone executors; vamos por la JVM:
mem_status = sc._jsc.sc().getExecutorMemoryStatus()
print(f"Executors registrados: {mem_status.size()}")
it = mem_status.iterator()
while it.hasNext():
    entry = it.next()
    host = entry._1()
    remaining, max_mem = entry._2()._1(), entry._2()._2()
    print(f"  - {host}  free={remaining/1e9:.2f} GB  max={max_mem/1e9:.2f} GB")

print()
print("Default parallelism :", sc.defaultParallelism)
print("Default min parts   :", sc.defaultMinPartitions)
"""),

    md("## 3. HDFS: contenido de `/usr/proyecto/bronze/`"),
    code("""
# Usamos el FileSystem de Hadoop expuesto por la JVM de Spark
URI = sc._jvm.java.net.URI
Path = sc._jvm.org.apache.hadoop.fs.Path
FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem

fs = FileSystem.get(URI(HDFS_NAMENODE), sc._jsc.hadoopConfiguration())

def listdir(hdfs_dir):
    files = fs.listStatus(Path(hdfs_dir))
    return [(f.getPath().getName(), f.getLen(), f.isDirectory()) for f in files]

for sub in ["bronze", "bronze_parquet", "silver", "gold"]:
    p = f"/usr/proyecto/{sub}"
    try:
        items = listdir(p)
        print(f"\\n=== {p} ===")
        for name, size, is_dir in items:
            tag = "DIR " if is_dir else "FILE"
            human = f"{size/1e6:>10.1f} MB" if not is_dir else " " * 12
            print(f"  {tag}  {human}  {name}")
    except Exception as e:
        print(f"\\n=== {p} ===  (vacío o no existe)  {e}")
"""),

    md("## 4. Sanity check: leer MEN (5 MB) desde HDFS y mostrar cabeceras"),
    code("""
# MEN es el más pequeño — buena verificación end-to-end sin gastar recursos.
df_men = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("multiLine", True)
    .option("quote", '\"')
    .option("escape", '\"')
    .csv(f"hdfs://10.43.97.164:9000{P.BRONZE_CSV_MEN}")
)
print(f"MEN  ->  rows={df_men.count():,}   cols={len(df_men.columns)}")
df_men.printSchema()
df_men.show(3, truncate=40)
"""),

    md("## 5. Sanity check ICFES (head only, sin contar)"),
    code("""
# Solo leemos schema + 5 filas (sin .count() para no cargar 3.5 GB).
df_icfes = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("multiLine", True)
    .option("quote", '\"')
    .option("escape", '\"')
    .csv(f"hdfs://10.43.97.164:9000{P.BRONZE_CSV_ICFES}")
)
print(f"ICFES columnas: {len(df_icfes.columns)}")
print("Primeras 12 columnas:", df_icfes.columns[:12])
df_icfes.select("PERIODO", "COLE_COD_MCPIO_UBICACION", "COLE_DEPTO_UBICACION",
                "COLE_AREA_UBICACION", "FAMI_TIENEINTERNET", "PUNT_GLOBAL").show(5, truncate=False)
"""),

    md("""
## 6. Conclusión

Si todas las celdas anteriores ejecutaron sin error:

- ✅ El SparkSession se conecta al cluster `spark://spark-master:7077`.
- ✅ HDFS responde en `hdfs://10.43.97.164:9000` y los CSVs Bronze están allí.
- ✅ Spark puede leer CSV desde HDFS respetando UTF-8.

**Siguiente paso:** notebook `01_bronze_csv_a_parquet.ipynb` — convierte los 4 CSVs a Parquet snappy.
"""),

    code("spark.stop()"),
]

if __name__ == "__main__":
    build(cells, NB_PATH)
