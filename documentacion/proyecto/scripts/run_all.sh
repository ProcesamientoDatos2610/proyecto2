#!/usr/bin/env bash
# Ejecuta todos los notebooks de la Entrega 2 en secuencia, en el remoto.
# Uso (desde el remoto): bash ~/proyecto_datos/scripts/run_all.sh
set -u  # cualquier var no definida es error; -e no porque queremos seguir aunque uno falle

cd /home/estudiante/proyecto_datos/notebooks_entrega2

NOTEBOOKS=(
    "00_setup_verificacion.ipynb"
    "01_bronze_csv_a_parquet.ipynb"
    "02_silver_icfes.ipynb"
    "03_silver_internet.ipynb"
    "04_silver_sisben.ipynb"
    "05_silver_men.ipynb"
    "06_gold_panel_municipal.ipynb"
    "07_eda_y_preguntas_negocio.ipynb"
    "08_ml_supervisado_rf.ipynb"
    "09_ml_no_supervisado_kmeans.ipynb"
)

mkdir -p /tmp/nb_logs

for nb in "${NOTEBOOKS[@]}"; do
    log=/tmp/nb_logs/${nb%.ipynb}.log
    printf "\n=========== EJECUTANDO %s ===========\n" "$nb"
    start=$(date +%s)
    if jupyter nbconvert --to notebook --execute --inplace \
            --ExecutePreprocessor.timeout=2400 "$nb" >"$log" 2>&1; then
        printf "OK  %s  (%ds, log: %s)\n" "$nb" "$(( $(date +%s) - start ))" "$log"
    else
        printf "FAIL  %s  (%ds, log: %s)\n" "$nb" "$(( $(date +%s) - start ))" "$log"
        printf "----- últimas líneas del log -----\n"
        tail -25 "$log"
        printf "----------------------------------\n"
        printf "Deteniendo cadena en %s. Corrige y reintenta.\n" "$nb"
        exit 1
    fi
done

printf "\nTodos los notebooks ejecutaron OK.\n"
