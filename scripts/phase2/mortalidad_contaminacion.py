# arvchivo 4
# ==========================================================
# CONEXIÓN FACT ↔ CONTAMINACIÓN con relleno 1–a–1 (PERSISTE)
# Objetivo:
#   - Estandarizar fecha_hora en contaminación
#   - Mapear estacion_norm → PK_ESTACION
#   - Unir por (FECHA_HORA, PK_ESTACION)
#   - Rellenar faltantes con valores artificiales coherentes
#   - Crear dimContaminacion (PK_CONTAMINACION) y FK_CONTAMINACION en fact
#   - Persistir resultados
# ==========================================================
import pandas as pd, numpy as np, os

RUTA_STD = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/"
RUTA_MD  = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"

df_cont = pd.read_csv(os.path.join(RUTA_STD, "PM25_2021_clean.csv"))
df_fact = pd.read_csv(os.path.join(RUTA_MD,  "factFallecimientos.csv"))
dim_est = pd.read_csv(os.path.join(RUTA_MD,  "dimEstacionMedicion.csv"))

# 1) Normalizar fecha-hora (redondeada a hora exacta, igual que fact)
df_cont = df_cont.copy()
df_cont["FECHA_HORA"] = pd.to_datetime(df_cont["fecha_hora"]).dt.strftime("%Y-%m-%d %H:00:00")

# 2) Mapear estacion_norm → PK_ESTACION
cont_key = df_cont.merge(
    dim_est[["PK_ESTACION","estacion_norm"]],
    on="estacion_norm", how="left"
)

# 3) Unir por (FECHA_HORA, PK_ESTACION)
preview = df_fact.merge(
    cont_key[["FECHA_HORA","PK_ESTACION","pm25_concentracion","pm25_media_movil","iboca"]],
    left_on=["FECHA_HORA","FK_ESTACION"],
    right_on=["FECHA_HORA","PK_ESTACION"],
    how="left"
)

# ================== PRINTS: estado inicial =================
total = len(df_fact)
matched = preview["pm25_concentracion"].notna().sum()
unmatched = total - matched
print(f"[INFO] fact filas: {total}")
print(f"[INFO] matches reales (tiempo+estación): {matched}  ({matched/total:.2%})")
print(f"[WARN] sin match inicial: {unmatched}  ({unmatched/total:.2%})\n")

# ==========================================================
# 4) Construir dimContaminacion con filas reales
#    + generar filas artificiales para los sin match
# ==========================================================
# Base real (única por FECHA_HORA + PK_ESTACION)
real_dim = preview.loc[
    preview["pm25_concentracion"].notna(),
    ["FECHA_HORA","FK_ESTACION","pm25_concentracion","pm25_media_movil","iboca"]
].drop_duplicates().rename(columns={"FK_ESTACION":"PK_ESTACION"}).copy()

# Detectar faltantes (necesitan fila artificial)
faltantes_df = preview.loc[
    preview["pm25_concentracion"].isna(),
    ["FECHA_HORA","FK_ESTACION"]
].copy()

# Generar valores artificiales reproducibles
rng = np.random.RandomState(123)
art_conc = rng.uniform(5, 60, size=len(faltantes_df))
art_mm   = np.clip(art_conc + rng.normal(0, 3, size=len(faltantes_df)), 5, 60)
art_ib   = rng.uniform(50, 120, size=len(faltantes_df))

art_dim = faltantes_df.copy()
art_dim.rename(columns={"FK_ESTACION":"PK_ESTACION"}, inplace=True)
art_dim["pm25_concentracion"] = art_conc.round(2)
art_dim["pm25_media_movil"]   = art_mm.round(2)
art_dim["iboca"]              = art_ib.round(2)

# dimContaminacion = reales ∪ artificiales
dim_cont = pd.concat([real_dim, art_dim], ignore_index=True)

# Crear PK_CONTAMINACION
dim_cont = dim_cont.drop_duplicates(subset=["FECHA_HORA","PK_ESTACION"]).reset_index(drop=True)
dim_cont["PK_CONTAMINACION"] = range(1, len(dim_cont)+1)

# ================== PRINTS: dimensión construida ===========
print("[OK] dimContaminacion construida (reales + artificiales)")
print("     filas totales en dimContaminacion:", len(dim_cont))
print(dim_cont.head(10), "\n")

# ==========================================================
# 5) Asignar FK_CONTAMINACION a cada fila del fact
#    (merge 1–a–1 por FECHA_HORA + estación)
# ==========================================================
key_map = dim_cont[["FECHA_HORA","PK_ESTACION","PK_CONTAMINACION"]].copy()

df_fact = df_fact.merge(
    key_map,
    left_on=["FECHA_HORA","FK_ESTACION"],
    right_on=["FECHA_HORA","PK_ESTACION"],
    how="left"
)

df_fact.rename(columns={"PK_CONTAMINACION":"FK_CONTAMINACION"}, inplace=True)
df_fact.drop(columns=["PK_ESTACION"], inplace=True)

# ================== PRINTS: validaciones ===================
faltan_fk = df_fact["FK_CONTAMINACION"].isna().sum()
print(f"[CHECK] filas en fact sin FK_CONTAMINACION:", faltan_fk)
assert faltan_fk == 0, "Existen filas sin FK_CONTAMINACION; revisar llaves."

# Chequeo de correspondencia 1–a–1
dups = df_fact[["FECHA_HORA","FK_ESTACION","FK_CONTAMINACION"]].duplicated().sum()
print(f"[CHECK] duplicados en (FECHA_HORA, FK_ESTACION, FK_CONTAMINACION): {dups}")

print("\n[CHECK] Muestra fact con FK_CONTAMINACION:")
print(df_fact[["FECHA_HORA","FK_ESTACION","FK_CONTAMINACION"]].head(10), "\n")

# ==========================================================
# 6) Persistir: dimContaminacion y factFallecimientos
# ==========================================================
dim_cont_path = os.path.join(RUTA_MD, "dimContaminacion.csv")
fact_path     = os.path.join(RUTA_MD, "factFallecimientos.csv")

# Ordenar dimensión por tiempo y estación para lectura humana
dim_cont = dim_cont.sort_values(["FECHA_HORA","PK_ESTACION"]).reset_index(drop=True)

dim_cont.to_csv(dim_cont_path, index=False)
df_fact.to_csv(fact_path, index=False)

print("[OK] Archivos guardados correctamente:")
print(" -", dim_cont_path)
print(" -", fact_path)