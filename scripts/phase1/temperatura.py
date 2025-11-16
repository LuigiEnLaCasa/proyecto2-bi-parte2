# ------------------------------------------------------------
# Limpieza temperatura_2021.csv — phase1 (standard_spacetime)
# ------------------------------------------------------------
# 1) Columnas a conservar:
#    - mes_num, Enos, Temperatura promedio, Temperatura Mínima,
#      Temperatura Máxima.
#    - Eliminar columnas: Año, Mes, mes_nombre, fecha_mes.
#
# 2) Nombres y formato:
#    - Renombrar columnas a snake_case:
#      mes_num → mes, Enos → enos,
#      Temperatura promedio → temperatura_promedio,
#      Temperatura Mínima → temperatura_minima,
#      Temperatura Máxima → temperatura_maxima.
#
# 3) Conversión de tipos:
#    - Reemplazar comas por puntos decimales y convertir
#      todas las temperaturas a float.
#
# 4) Columnas finales (phase1):
#    mes, enos, temperatura_promedio, temperatura_minima, temperatura_maxima
#
# 5) Salida:
#    - Guardar archivo limpio en:
#      /Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime
# ------------------------------------------------------------



# ------------------------------------------------------------
# temperatura_2021.csv — phase1 (standard_spacetime)
# Paso 1: importación de librerías y lectura del CSV
# ------------------------------------------------------------

import pandas as pd
import numpy as np

# Ruta del archivo raw
path_raw = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/raw/temperatura_2021.csv"

# Cargar CSV en DataFrame
df_temp = pd.read_csv(path_raw, encoding="utf-8")

# Mostrar primeras filas para verificación inicial
# print("=== HEAD INICIAL ===")
# print(df_temp.head())


# ------------------------------------------------------------
# Paso 2 — limpieza de columnas (nombres y eliminación)
# ------------------------------------------------------------

# Renombrar columnas a formato snake_case y sin tildes
df_temp = df_temp.rename(columns={
    "Año": "anio",
    "Mes": "mes",
    "Temperatura Máxima": "temp_max",
    "Temperatura Mínima": "temp_min",
    "Temperatura promedio": "temp_prom",
    "Enos ": "enos",
    "mes_nombre": "mes_nombre",
    "mes_num": "mes_num",
    "fecha_mes": "fecha_mes"
})

# Eliminar columnas que no necesitamos
df_temp = df_temp.drop(columns=["anio", "mes_nombre", "fecha_mes"])

# print("\n=== Columnas después de limpieza ===")
# print(df_temp.columns.tolist())

# Mostrar primeras filas para verificar
# print("\n=== HEAD actualizado ===")
# print(df_temp.head())

# ------------------------------------------------------------
# Paso 3 — conversión de columnas de temperatura a float
# ------------------------------------------------------------

cols_temp = ["temp_max", "temp_min", "temp_prom"]

for col in cols_temp:
    df_temp[col] = (
        df_temp[col]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

# print("\n=== Tipos de datos tras conversión ===")
# print(df_temp[cols_temp].dtypes)

# print("\n=== HEAD tras conversión ===")
# print(df_temp.head())

# ------------------------------------------------------------
# Paso final Fase 1 — guardar archivo limpio (standard_spacetime)
# ------------------------------------------------------------

out_path = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/temperatura_2021_clean.csv"
df_temp.to_csv(out_path, index=False, encoding="utf-8")

print("\nArchivo guardado en fase intermedia →", out_path)
print("Tamaño final:", df_temp.shape)