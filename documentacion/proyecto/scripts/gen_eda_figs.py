"""Genera ~8 figuras EDA para la sección 'Exploración y limpieza' del documento LaTeX.
Lee Bronze/Silver Parquet desde HDFS y produce PNGs en /home/estudiante/proyecto_datos/evidencia/eda/.
"""
import os, sys
sys.path.insert(0, "/home/estudiante/proyecto_datos/scripts")
from common_spark import get_spark, P
from pyspark.sql import functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT = "/home/estudiante/proyecto_datos/evidencia/eda"
os.makedirs(OUT, exist_ok=True)

spark = get_spark("EDA-Figs", executor_memory="3g", driver_memory="3g", cores=2)
print("Spark listo.")

# ===== ICFES =====
icfes = spark.read.parquet(P.SILVER_ICFES)

# F1 — Histograma PUNT_GLOBAL (muestreo para hacer cabe en pandas)
print("Fig 1: histograma PUNT_GLOBAL")
sample = icfes.select("PUNT_GLOBAL").sample(0.05, seed=42).toPandas()
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.hist(sample["PUNT_GLOBAL"].dropna(), bins=50, color="#3b6fb0", edgecolor="white", alpha=0.9)
ax.axvline(sample["PUNT_GLOBAL"].mean(), color="darkred", linestyle="--", linewidth=2, label=f"Media: {sample['PUNT_GLOBAL'].mean():.0f}")
ax.set_xlabel("Puntaje global Saber 11"); ax.set_ylabel("Frecuencia")
ax.set_title("Distribución del puntaje global Saber 11 (muestra 5%)")
ax.legend(); ax.grid(alpha=0.3)
plt.tight_layout(); plt.savefig(f"{OUT}/icfes_hist_punt_global.png", dpi=120); plt.close()

# F2 — Puntaje promedio por departamento (top 15)
print("Fig 2: puntaje promedio por departamento")
dept = (icfes.groupBy("COLE_DEPTO_UBICACION")
             .agg(F.round(F.avg("PUNT_GLOBAL"), 1).alias("avg_punt"),
                  F.count("*").alias("n"))
             .filter(F.col("n") >= 1000)
             .orderBy(F.desc("avg_punt"))
             .limit(20)
             .toPandas())
fig, ax = plt.subplots(figsize=(9, 6))
colors = ["#2ca02c" if i < 5 else "#7f7f7f" if i < 15 else "#d62728" for i in range(len(dept))]
ax.barh(dept["COLE_DEPTO_UBICACION"][::-1], dept["avg_punt"][::-1], color=colors[::-1])
ax.set_xlabel("Puntaje global promedio")
ax.set_title("Puntaje global promedio Saber 11 por departamento (top 20)")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout(); plt.savefig(f"{OUT}/icfes_depto_top.png", dpi=120); plt.close()

# F3 — Internet vs puntaje promedio (urbano/rural)
print("Fig 3: internet vs puntaje por zona")
df3 = (icfes.groupBy("FAMI_TIENEINTERNET", "COLE_AREA_UBICACION")
            .agg(F.round(F.avg("PUNT_GLOBAL"), 1).alias("avg_punt"),
                 F.count("*").alias("n"))
            .filter(F.col("COLE_AREA_UBICACION").isin("URBANO","RURAL"))
            .filter(F.col("FAMI_TIENEINTERNET").isin("SI","NO","SIN INFORMACION"))
            .toPandas())
print(df3)
pivot = df3.pivot(index="FAMI_TIENEINTERNET", columns="COLE_AREA_UBICACION", values="avg_punt")
pivot = pivot.reindex(["SI","NO","SIN INFORMACION"])
fig, ax = plt.subplots(figsize=(8, 5))
x = np.arange(len(pivot.index)); w = 0.38
ax.bar(x - w/2, pivot["URBANO"], w, label="Urbano", color="#3b6fb0")
ax.bar(x + w/2, pivot["RURAL"],  w, label="Rural",  color="#d97a3a")
ax.set_xticks(x); ax.set_xticklabels(pivot.index)
ax.set_xlabel("Internet en el hogar"); ax.set_ylabel("Puntaje global promedio")
ax.set_title("Puntaje Saber 11 según acceso a internet y zona del colegio")
ax.legend(); ax.grid(axis="y", alpha=0.3)
for i, lbl in enumerate(pivot.index):
    for j, zone in enumerate(["URBANO","RURAL"]):
        v = pivot.loc[lbl, zone]
        if v is not None and not np.isnan(v):
            ax.text(i + (j-0.5)*w, v + 1, f"{v:.0f}", ha="center", fontsize=9)
plt.tight_layout(); plt.savefig(f"{OUT}/icfes_internet_zona.png", dpi=120); plt.close()

# ===== INTERNET FIJO =====
inet = spark.read.parquet(P.SILVER_INTERNET)

# F4 — Evolución total accesos por año
print("Fig 4: accesos internet por año")
year = (inet.groupBy("ANO").agg(F.sum("NUM_ACCESOS").alias("total")).orderBy("ANO").toPandas())
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(year["ANO"], year["total"]/1e6, "o-", color="#3b6fb0", linewidth=2, markersize=8)
ax.fill_between(year["ANO"], 0, year["total"]/1e6, alpha=0.2, color="#3b6fb0")
ax.set_xlabel("Año"); ax.set_ylabel("Accesos totales (millones)")
ax.set_title("Evolución del total de accesos a internet fijo en Colombia (Silver)")
ax.grid(alpha=0.3)
for x, y in zip(year["ANO"], year["total"]/1e6):
    ax.text(x, y + 0.05, f"{y:.1f}M", ha="center", fontsize=8)
plt.tight_layout(); plt.savefig(f"{OUT}/internet_evolucion_anual.png", dpi=120); plt.close()

# F5 — Top tecnologías (% del total)
print("Fig 5: top tecnologias")
tech = (inet.groupBy("TECNOLOGIA").agg(F.sum("NUM_ACCESOS").alias("total"))
            .orderBy(F.desc("total")).limit(8).toPandas())
tech["pct"] = tech["total"] / tech["total"].sum() * 100
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh(tech["TECNOLOGIA"][::-1], tech["pct"][::-1], color="#7b4ca6")
ax.set_xlabel("% del total de accesos")
ax.set_title("Distribución de accesos a internet fijo por tecnología (top 8)")
ax.grid(axis="x", alpha=0.3)
for i, v in enumerate(tech["pct"][::-1]):
    ax.text(v + 0.5, i, f"{v:.1f}%", va="center", fontsize=9)
plt.tight_layout(); plt.savefig(f"{OUT}/internet_top_tecnologias.png", dpi=120); plt.close()

# ===== SISBEN =====
sisb = spark.read.parquet(P.SILVER_SISBEN_MPIO)

# F6 — Distribución promedio por grupo SISBEN (a nivel municipio)
print("Fig 6: distribución por grupo SISBEN")
grupos = sisb.agg(
    F.round(F.avg("pct_grupo_A")*100, 1).alias("A"),
    F.round(F.avg("pct_grupo_B")*100, 1).alias("B"),
    F.round(F.avg("pct_grupo_C")*100, 1).alias("C"),
    F.round(F.avg("pct_grupo_D")*100, 1).alias("D"),
).first().asDict()
fig, ax = plt.subplots(figsize=(7, 4.5))
colors = ["#d62728", "#ff7f0e", "#ffd92f", "#2ca02c"]
labels = [f"Grupo {k}" for k in ["A","B","C","D"]]
values = [grupos[k] for k in ["A","B","C","D"]]
ax.bar(labels, values, color=colors)
ax.set_ylabel("% promedio de personas (por municipio)")
ax.set_title("Distribución promedio de personas por grupo Sisbén")
ax.grid(axis="y", alpha=0.3)
for i, v in enumerate(values):
    ax.text(i, v + 0.5, f"{v:.1f}%", ha="center", fontsize=10)
plt.tight_layout(); plt.savefig(f"{OUT}/sisben_distribucion_grupos.png", dpi=120); plt.close()

# F7 — Índice de privación vs puntaje global municipal (scatter)
print("Fig 7: privacion vs puntaje global")
panel = spark.read.parquet(P.GOLD_PANEL_MUNICIPAL).filter(F.col("avg_punt_global").isNotNull())
pf = panel.select("idx_privacion","avg_punt_global","n_estudiantes","pct_rural_sisben").toPandas().dropna()
fig, ax = plt.subplots(figsize=(8, 5))
sc_ = ax.scatter(pf["idx_privacion"], pf["avg_punt_global"],
                 s=pf["n_estudiantes"].clip(upper=2000)/10,
                 c=pf["pct_rural_sisben"], cmap="RdYlGn_r",
                 alpha=0.5, edgecolors="none")
ax.set_xlabel("Índice de privación Sisbén (suma de 15 indicadores)")
ax.set_ylabel("Puntaje global promedio (municipio-año)")
ax.set_title("Privación Sisbén vs Saber 11 — color = % rural")
cb = plt.colorbar(sc_, ax=ax); cb.set_label("% rural")
ax.grid(alpha=0.3)
plt.tight_layout(); plt.savefig(f"{OUT}/sisben_priv_vs_punt.png", dpi=120); plt.close()

# ===== MEN =====
men = spark.read.parquet(P.SILVER_MEN)

# F8 — Evolución cobertura neta por año (promedio nacional)
print("Fig 8: evolución cobertura MEN")
ev = (men.groupBy("ANO").agg(
    F.round(F.avg("COBERTURA_NETA")*100, 2).alias("cobertura_neta"),
    F.round(F.avg("COBERTURA_NETA_PRIMARIA")*100, 2).alias("primaria"),
    F.round(F.avg("COBERTURA_NETA_SECUNDARIA")*100, 2).alias("secundaria"),
    F.round(F.avg("COBERTURA_NETA_MEDIA")*100, 2).alias("media"),
).orderBy("ANO").toPandas())
fig, ax = plt.subplots(figsize=(9, 5))
ax.plot(ev["ANO"], ev["primaria"], "o-", label="Primaria", linewidth=2)
ax.plot(ev["ANO"], ev["secundaria"], "s-", label="Secundaria", linewidth=2)
ax.plot(ev["ANO"], ev["media"], "^-", label="Media", linewidth=2)
ax.plot(ev["ANO"], ev["cobertura_neta"], "d--", label="Neta total", linewidth=2, color="black")
ax.set_xlabel("Año"); ax.set_ylabel("Cobertura neta promedio (%)")
ax.set_title("Evolución de la cobertura neta MEN por nivel educativo (2011-2024)")
ax.legend(); ax.grid(alpha=0.3)
plt.tight_layout(); plt.savefig(f"{OUT}/men_evolucion_cobertura.png", dpi=120); plt.close()

print(f"\nFiguras generadas en {OUT}:")
for f in sorted(os.listdir(OUT)):
    print(" ", f)

spark.stop()
