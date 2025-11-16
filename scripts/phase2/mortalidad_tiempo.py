# ARCHIVO 2

# ==========================================================
# Carga de librerías y del archivo fact_fallecimientos.csv
# ==========================================================
# ==========================================================
# PASO — Enriquecer factFallecimientos SOLO con día y hora
# ==========================================================
import pandas as pd, numpy as np, os, calendar

RUTA_MULTIDIM = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"
fact_path = os.path.join(RUTA_MULTIDIM, "factFallecimientos.csv")

fact = pd.read_csv(fact_path)
print("[INFO] Cargado factFallecimientos.csv")
print("shape inicial:", fact.shape)

rng = np.random.RandomState(42)
ANIO_DEF = 2021
dias_mes = {m: calendar.monthrange(ANIO_DEF, m)[1] for m in range(1,13)}

fact = fact.copy()
fact["DIA_FALLECIMIENTO"]  = fact["MES"].map(dias_mes).apply(lambda d: rng.randint(1, d+1))
fact["HORA_FALLECIMIENTO"] = rng.randint(0, 24, size=len(fact))

# ================== PRINTS DE CONTROL =====================
# print("[CHECK] shape final (debe ser igual):", fact.shape)
# print("[CHECK] nuevas columnas presentes:",
#       {"DIA_FALLECIMIENTO","HORA_FALLECIMIENTO"}.issubset(fact.columns))
# print("[CHECK] muestra:")
# print(fact[["MES","DIA_FALLECIMIENTO","HORA_FALLECIMIENTO"]].head(10))
# ==========================================================
# VERIFICACIÓN VISUAL — Revisar todas las columnas de fact
# ==========================================================
# print("[INFO] Vista general de factFallecimientos con nuevas columnas:")
# print(fact.head())  # muestra todas las columnas
# print("\n[INFO] Total de columnas:", len(fact.columns))
# print("[INFO] Nombres de columnas:")
# print(list(fact.columns))


# Vista previa dimTiempo (en memoria, sin id y sin guardar)
dt = pd.to_datetime(
    dict(year=2021, month=fact["MES"],
         day=fact["DIA_FALLECIMIENTO"], hour=fact["HORA_FALLECIMIENTO"])
).dt.strftime("%Y-%m-%d %H:00:00")   # <-- corrección aquí

dim_tiempo = pd.DataFrame({"FECHA_HORA": dt}).drop_duplicates().copy()
s = pd.to_datetime(dim_tiempo["FECHA_HORA"])
dim_tiempo["anio"] = s.dt.year
dim_tiempo["mes"] = s.dt.month
dim_tiempo["trimestre"] = s.dt.quarter
dim_tiempo["dia"] = s.dt.day
dim_tiempo["hora"] = s.dt.hour

print(dim_tiempo.head())



# ==========================================================
# PASO — Crear PK_TIEMPO en dim_tiempo y FK_TIEMPO en fact
# ==========================================================

# 1. primary key en la dimensión de tiempo
dim_tiempo = dim_tiempo.copy()
dim_tiempo["PK_TIEMPO"] = range(1, len(dim_tiempo) + 1)

# 2. generar FECHA_HORA en fact (mismo formato que dim_tiempo)
fact["FECHA_HORA"] = pd.to_datetime(
    dict(
        year=2021,
        month=fact["MES"],
        day=fact["DIA_FALLECIMIENTO"],
        hour=fact["HORA_FALLECIMIENTO"]
    )
).dt.strftime("%Y-%m-%d %H:00:00")

# 3. merge para asignar la FK_TIEMPO
fact = fact.merge(
    dim_tiempo[["FECHA_HORA", "PK_TIEMPO"]],
    on="FECHA_HORA",
    how="left"
)
fact.rename(columns={"PK_TIEMPO": "FK_TIEMPO"}, inplace=True)

# ================== PRINTS DE CONTROL =====================
print("[CHECK] dim_tiempo con PK_TIEMPO:")
print(dim_tiempo.head())

print("\n[CHECK] factFallecimientos con FK_TIEMPO asignada:")
print(fact.head())


# ==========================================================
# GUARDAR dimTiempo y factFallecimientos actualizados
# ==========================================================
# ==========================================================
# GUARDAR dimTiempo (ordenada por tiempo) y factFallecimientos
# ==========================================================
import os

ruta = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"

# ordenar la dimensión por fecha_hora antes de persistir
dim_tiempo = dim_tiempo.sort_values("FECHA_HORA").reset_index(drop=True)

# persistir
dim_tiempo.to_csv(os.path.join(ruta, "dimTiempo.csv"), index=False)
fact.to_csv(os.path.join(ruta, "factFallecimientos.csv"), index=False)

print("[OK] Archivos guardados correctamente y dimTiempo ordenada cronológicamente.")