# ------------------------------------------------------------
# Limpieza precipitacion_2021.csv — phase1 (standard_spacetime)
# ------------------------------------------------------------
# 1) Tiempo mensual:
#    - Parsear fecha_mes y derivar year_month = YYYY-MM (ej. 2021-01).
#    - Mantener solo el mes; eliminar día y año explícitos si se repiten.
#
# 2) Tipos y nombres:
#    - Cast precipitacion_mm a float.
#    - Renombrar columnas a snake_case.
#
# 3) Columnas finales (phase1):
#    year_month, precipitacion_mm
#
# 4) Salida:
#    - Guardar archivo mensual limpio en:
#      /Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime
# ------------------------------------------------------------


# ------------------------------------------------------------
# precipitacion_2021.csv — phase1 (standard_spacetime)
# Paso 1: importación de librerías y lectura del CSV
# ------------------------------------------------------------

import pandas as pd
import numpy as np

# Ruta del archivo raw
path_raw = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/raw/precipitacion_2021.csv"

# Cargar CSV en DataFrame
df_prec = pd.read_csv(path_raw, encoding="utf-8")

# Mostrar primeras filas para verificación inicial
print("=== HEAD INICIAL ===")
print(df_prec.head())

# ------------------------------------------------------------
# Paso final Fase 1 — guardar archivo limpio (standard_spacetime)
# ------------------------------------------------------------

out_path = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/precipitacion_2021_clean.csv"
df_prec.to_csv(out_path, index=False, encoding="utf-8")

print("\nArchivo guardado en fase intermedia →", out_path)
print("Tamaño final:", df_prec.shape)