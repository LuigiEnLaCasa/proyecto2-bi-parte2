# ARCHIVO 1 
# ------------------------------------------------------------
# Fase 2 — Hecho ↔ Dimensión Ambiente (preparación)
# Solo importaciones y rutas
# ------------------------------------------------------------
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime")

# ------------------------------------------------------------
# Fase 2 — Importación de datos base para construir dimensión Ambiente
# ------------------------------------------------------------

# Archivos fuente
path_mortalidad = BASE / "MortalidadPrematura2021_clean.csv"
path_precipitacion = BASE / "precipitacion_2021_clean.csv"
path_temperatura = BASE / "temperatura_2021_clean.csv"

# Cargar DataFrames
df_mortalidad = pd.read_csv(path_mortalidad, encoding="utf-8")
df_prec = pd.read_csv(path_precipitacion, encoding="utf-8")
df_temp = pd.read_csv(path_temperatura, encoding="utf-8")

# Verificación rápida
# print("=== HEAD Mortalidad ===")
# print(df_mortalidad.head(), "\n")

# print("=== HEAD Precipitación ===")
# print(df_prec.head(), "\n")

# print("=== HEAD Temperatura ===")
# print(df_temp.head(), "\n")


# ------------------------------------------------------------
# Paso 3 — crear columna mes_num en df_prec (para empatar con temperatura)
# ------------------------------------------------------------

# Convertir fecha_mes a tipo datetime y extraer número de mes
df_prec["fecha_mes"] = pd.to_datetime(df_prec["fecha_mes"], errors="coerce")
df_prec["mes_num"] = df_prec["fecha_mes"].dt.month

# print("\n=== HEAD actualizado de Precipitación ===")
# print(df_prec.head())


# ------------------------------------------------------------
# Paso 4 — crear dimensión Ambiente (PK_ambiente)
# ------------------------------------------------------------

# Unir temperatura + precipitación por mes_num
dimAmbiente = pd.merge(
    df_temp,
    df_prec[["mes_num", "precipitacion_mm"]],
    on="mes_num",
    how="inner"
)

# Crear clave primaria surrogate
dimAmbiente.insert(0, "PK_ambiente", range(1, len(dimAmbiente) + 1))

# print("\n=== HEAD dimAmbiente ===")
# print(dimAmbiente.head(12))

# print("\n=== Columnas finales ===")
# print(dimAmbiente.columns.tolist())


# ------------------------------------------------------------
# Paso 5 — asignar FK_ambiente a cada registro de mortalidad
# ------------------------------------------------------------

# Crear tabla de correspondencia mes ↔ PK_ambiente
mapa_mes_ambiente = (
    dimAmbiente[["mes_num", "PK_ambiente"]]
    .set_index("mes_num")["PK_ambiente"]
    .to_dict()
)

# Crear columna FK_ambiente en df_mortalidad
df_mortalidad["FK_ambiente"] = df_mortalidad["MES"].map(mapa_mes_ambiente)

# Verificar resultados
# print("\n=== HEAD con FK_ambiente ===")
# print(df_mortalidad[["MES", "FK_ambiente"]].head())

# print("\n=== Conteo por FK_ambiente ===")
# print(df_mortalidad["FK_ambiente"].value_counts().sort_index())


# ------------------------------------------------------------
# Corrección — eliminar columna REGIMEN_SEGURIDAD_SOCIAL
# ------------------------------------------------------------

if "REGIMEN_SEGURIDAD_SOCIAL" in df_mortalidad.columns:
    df_mortalidad = df_mortalidad.drop(columns=["REGIMEN_SEGURIDAD_SOCIAL"])
    print("Columna 'REGIMEN_SEGURIDAD_SOCIAL' eliminada correctamente.")
else:
    print("La columna 'REGIMEN_SEGURIDAD_SOCIAL' no existe o ya fue eliminada.")

# ------------------------------------------------------------
# Limpiar dimAmbiente: quitar columnas de tiempo ('mes', 'mes_num')
# y re-guardar el CSV en la carpeta multidimensional
# ------------------------------------------------------------

cols_a_borrar = [c for c in ["mes", "mes_num"] if c in dimAmbiente.columns]
if cols_a_borrar:
    dimAmbiente = dimAmbiente.drop(columns=cols_a_borrar)
    print("Columnas eliminadas de dimAmbiente:", cols_a_borrar)
else:
    print("dimAmbiente no contenía columnas de tiempo.")

print("\nColumnas finales dimAmbiente:", dimAmbiente.columns.tolist())

# ------------------------------------------------------------
# Paso final — guardar dimAmbiente y factFallecimientos
# ------------------------------------------------------------

# Rutas de salida
out_ambiente = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/dimAmbiente.csv"
out_fallecimientos = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/factFallecimientos.csv"

# Guardar DataFrames
dimAmbiente.to_csv(out_ambiente, index=False, encoding="utf-8")
df_mortalidad.to_csv(out_fallecimientos, index=False, encoding="utf-8")

print("\nArchivos guardados en carpeta multidimensional:")
print(" -", out_ambiente)
print(" -", out_fallecimientos)