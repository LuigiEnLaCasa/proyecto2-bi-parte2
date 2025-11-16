# ==========================================================
# DEPURAR Y REORDENAR factFallecimientos
# Objetivo:
#   - Eliminar columnas redundantes (ya modeladas en dimensiones)
#   - Eliminar CIE10_AGRUPADA (solo hay un valor)
#   - Reordenar columnas colocando FKs primero
#   - Persistir CSV final
# ==========================================================
import pandas as pd
import os

# --- Ruta base ---
RUTA_MD = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"
fact_path = os.path.join(RUTA_MD, "factFallecimientos.csv")

# --- Cargar dataset ---
df = pd.read_csv(fact_path)
print("[INFO] Cargado factFallecimientos.csv")
print("shape inicial:", df.shape)
print("[INFO] Columnas actuales:")
print(list(df.columns), "\n")

# ==========================================================
# PASO 1 — Eliminar columnas redundantes o temporales
# ==========================================================
cols_drop = [
    "MES",
    "DIA_FALLECIMIENTO",
    "HORA_FALLECIMIENTO",
    "FECHA_HORA",
    "nombre_localidad",
    "codigo_localidad",
    "CIE10_AGRUPADA"  # nueva eliminación solicitada
]

existentes = [c for c in cols_drop if c in df.columns]
df_clean = df.drop(columns=existentes)

print("[CHECK] Columnas eliminadas:", existentes)
print("shape después de limpieza:", df_clean.shape)
print("[CHECK] Columnas restantes:")
print(list(df_clean.columns), "\n")

# ==========================================================
# PASO 2 — Reordenar: FKs primero, luego datos del hecho
# ==========================================================
fk_candidates = ["FK_TIEMPO", "FK_ESTACION", "FK_CONTAMINACION", "FK_ambiente", "FK_AMBIENTE"]
existing_fks = [c for c in fk_candidates if c in df_clean.columns]
remaining_cols = [c for c in df_clean.columns if c not in existing_fks]
new_order = existing_fks + remaining_cols

df_reordered = df_clean.reindex(columns=new_order)

print("[CHECK] FKs detectadas y colocadas al inicio:", existing_fks)
print("[CHECK] Nuevas primeras columnas:", list(df_reordered.columns)[:10])
print("[CHECK] shape final:", df_reordered.shape, "\n")

# ==========================================================
# PASO 3 — Validación de CIE10_BASICA
# ==========================================================
if "CIE10_BASICA" in df_reordered.columns:
    print("[INFO] Valores únicos de CIE10_BASICA:")
    print(df_reordered["CIE10_BASICA"].dropna().unique())
    print("[CHECK] Total códigos únicos:", df_reordered["CIE10_BASICA"].nunique(), "\n")

# ==========================================================
# PASO 4 — Persistir CSV final
# ==========================================================
df_reordered.to_csv(fact_path, index=False)
print("[OK] factFallecimientos.csv depurado, reordenado y guardado correctamente.")
print("Ubicación:", fact_path)