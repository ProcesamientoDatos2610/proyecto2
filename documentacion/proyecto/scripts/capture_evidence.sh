#!/usr/bin/env bash
# Captura artefactos de evidencia: estado HDFS y Spark.
# Las screenshots reales (PNG) las debe tomar el usuario desde su browser;
# este script genera HTML/JSON/TXT que sirven como respaldo objetivo.
set -u
OUT=/home/estudiante/proyecto_datos/evidencia
mkdir -p "$OUT"

echo "Capturando estado HDFS..."
hdfs dfsadmin -report > "$OUT/hdfs_dfsadmin_report.txt" 2>&1
hdfs dfs -ls -R /usr/proyecto > "$OUT/hdfs_layout_proyecto.txt" 2>&1
hdfs dfs -du -h /usr/proyecto > "$OUT/hdfs_sizes_proyecto.txt" 2>&1
hdfs dfs -df -h / > "$OUT/hdfs_df.txt" 2>&1

echo "Capturando NameNode UI..."
curl -sS http://10.43.97.164:9870/ -o "$OUT/hdfs_namenode_home.html"
curl -sS "http://10.43.97.164:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" \
    | python3 -m json.tool > "$OUT/hdfs_namenode_jmx.json" 2>&1

echo "Capturando Spark Master UI..."
curl -sS http://10.43.97.164:8080/ -o "$OUT/spark_master_home.html"
curl -sS http://10.43.97.164:8080/json/ \
    | python3 -m json.tool > "$OUT/spark_master_state.json" 2>&1

# Spark History (apps completadas)
echo "Capturando últimas apps Spark..."
curl -sS http://10.43.97.164:8080/json/ \
    | python3 -c "
import json, sys
d = json.load(sys.stdin)
print('=== APPS DE ESTA EJECUCIÓN (más recientes 15) ===')
for a in sorted(d.get('completedapps',[]), key=lambda x: x.get('endtime',0), reverse=True)[:15]:
    dur = a.get('duration',0) / 1000
    print(f\"  {a.get('id'):30s}  {a.get('name'):30s}  {dur:7.1f}s  state={a.get('state')}\")
print()
print('=== WORKERS ===')
for w in d.get('workers',[]):
    print(f\"  {w.get('id')}  host={w.get('host')}  cores={w.get('cores')}  mem={w.get('memory')}MB  state={w.get('state')}\")
" > "$OUT/spark_apps_summary.txt"

echo "Capturando inventario de artefactos en HDFS..."
{
    echo "=== Bronze CSV (raw, ASCII renombrados) ==="; hdfs dfs -ls -h /usr/proyecto/bronze
    for d in icfes internet sisben men; do hdfs dfs -ls -h /usr/proyecto/bronze/$d; done
    echo; echo "=== Bronze Parquet ==="
    for d in icfes internet sisben men; do hdfs dfs -ls -h /usr/proyecto/bronze_parquet/$d; done
    echo; echo "=== Silver ==="
    hdfs dfs -ls -R /usr/proyecto/silver
    echo; echo "=== Gold ==="
    hdfs dfs -ls -R /usr/proyecto/gold
    echo; echo "=== Modelos ==="
    hdfs dfs -ls -R /usr/proyecto/models 2>&1
} > "$OUT/inventario_hdfs.txt"

echo "Listo. Evidencia en $OUT:"
ls -la "$OUT"
