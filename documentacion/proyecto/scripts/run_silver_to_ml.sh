#!/usr/bin/env bash
# Ejecuta los notebooks Silver -> Gold -> EDA -> ML en secuencia.
# (00 y 01 ya están hechos.)
set -u
cd /home/estudiante/proyecto_datos/notebooks_entrega2

NOTEBOOKS=(
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
        printf "FAIL %s  (%ds, log: %s)\n" "$nb" "$(( $(date +%s) - start ))" "$log"
        printf "----- últimas líneas del log -----\n"
        tail -40 "$log"
        printf "----------------------------------\n"
        exit 1
    fi
done
printf "\nCadena completa OK\n"
