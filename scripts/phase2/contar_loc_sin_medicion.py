# ==========================================================
# VERIFICAR CUÁNTOS REGISTROS TIENEN FK_ESTACION = 0
# ==========================================================
import pandas as pd
import os

RUTA_MULTIDIM = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"
fact_path = os.path.join(RUTA_MULTIDIM, "factFallecimientos.csv")

df_fact = pd.read_csv(fact_path)

sin_estacion = (df_fact["FK_ESTACION"] == 0).sum()
total = len(df_fact)

print(f"[INFO] Total registros: {total}")
print(f"[INFO] Registros con FK_ESTACION = 0: {sin_estacion}")
print(f"[INFO] Porcentaje: {sin_estacion/total:.2%}")