#Archivo 3
# ==========================================================
# PASO 1 — Cargar librerías y datasets base
# Objetivo:
#   - Importar librerías necesarias
#   - Cargar archivos de contaminación y fallecimientos
# ==========================================================

import pandas as pd
import numpy as np
import os

# --- Rutas principales ---
RUTA_STANDARD = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/"
RUTA_MULTIDIM = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/multidimensional/"

# --- Cargar datasets ---
contaminacion_path = os.path.join(RUTA_STANDARD, "PM25_2021_clean.csv")
fallecimientos_path = os.path.join(RUTA_MULTIDIM, "factFallecimientos.csv")

df_cont = pd.read_csv(contaminacion_path)
df_fact = pd.read_csv(fallecimientos_path)

# --- Prints de control ---
# print("[INFO] Cargado PM25_2021_clean.csv")
# print("shape:", df_cont.shape)
# print("columnas:", list(df_cont.columns), "\n")

# print("[INFO] Cargado factFallecimientos.csv")
# print("shape:", df_fact.shape)
# print("columnas:", list(df_fact.columns))

# ==========================================================
# PASO 2 — Extraer estaciones únicas desde contaminación
# Objetivo:
#   - Seleccionar columnas relevantes de puntos de medición
#   - Obtener un registro por estación (sin guardar)
#   - Ver cobertura de localidades vs fallecimientos
# ==========================================================

# --- columnas candidatas para la dimensión de estación ---
COLS_ESTACION = [
    "estacion_norm",      # clave limpia de estación (ya normalizada en tu fase 1)
    "estacion",           # nombre original
    "sigla",
    "direccion",
    "nombre_localidad",
    "codigo_localidad",
    "latitud",
    "longitud"
]

# --- selección segura (por si alguna no existe en el CSV) ---
cols_existentes = [c for c in COLS_ESTACION if c in df_cont.columns]
df_est_base = df_cont[cols_existentes].copy()

# print("[INFO] Contaminación: filas totales:", len(df_cont))
# print("[INFO] Contaminación: columnas usadas para estación:", cols_existentes)
print(df_est_base.head(5), "\n")

# --- un registro por estación ---
# Nota: aquí SÍ usamos drop_duplicates porque estamos formando la dimensión (valores únicos de estación)
subset_key = [c for c in ["estacion_norm"] if c in df_est_base.columns]
df_estaciones = df_est_base.drop_duplicates(subset=subset_key).reset_index(drop=True)

# print("[CHECK] Estaciones únicas detectadas:", len(df_estaciones))
# print(df_estaciones.head(10), "\n")

# --- cobertura por localidad: cuáles localidades del fact NO tienen estación ---
locs_fact = set(df_fact["codigo_localidad"].unique())
locs_est  = set(df_estaciones["codigo_localidad"].dropna().unique()) if "codigo_localidad" in df_estaciones.columns else set()

sin_estacion = sorted(locs_fact - locs_est)
# print("[CHECK] Localidades en fact:", len(locs_fact))
# print("[CHECK] Localidades con estación:", len(locs_est))
# print("[WARN] Localidades sin punto de medición (para mapeo a 'sin_medicion'):", sin_estacion[:20], "...")




# ==========================================================
# PASO 3 — Agregar registro especial "sin_medicion"
# Objetivo:
#   - Añadir fila genérica que represente localidades sin estación
#   - Mantener consistencia para el mapeo con factFallecimientos
# ==========================================================

# Crear DataFrame de un solo registro "sin_medicion"
sin_medicion = pd.DataFrame([{
    "estacion_norm": "sin_medicion",
    "estacion": "sin_medicion",
    "sigla": np.nan,
    "direccion": np.nan,
    "nombre_localidad": np.nan,
    "codigo_localidad": 0,
    "latitud": np.nan,
    "longitud": np.nan
}])

# Concatenar con df_estaciones
df_estaciones = pd.concat([sin_medicion, df_estaciones], ignore_index=True)

# Añadir clave primaria
df_estaciones["PK_ESTACION"] = range(0, len(df_estaciones))  # empieza en 0 para sin_medicion

# ================== PRINTS DE CONTROL =====================
# print("[INFO] Se añadió registro 'sin_medicion' a la dimensión.")
# print("[CHECK] Total estaciones ahora:", len(df_estaciones))
# print(df_estaciones.head(10))
# print("\n[CHECK] Últimas filas para validar PKs:")
# print(df_estaciones.tail(5))


# ==========================================================
# PASO 4 — Construir mapeo por localidad y asignar FK_ESTACION
# Objetivo:
#   - Elegir una estación representativa por codigo_localidad
#   - Asignar FK_ESTACION a cada fila del fact
#   - Localidades sin estación → FK_ESTACION = 0 (sin_medicion)
# ==========================================================

# Asegurar tipos enteros comparables
df_estaciones["codigo_localidad"] = df_estaciones["codigo_localidad"].fillna(0).astype(int)
df_fact["codigo_localidad"] = df_fact["codigo_localidad"].astype(int)

# Regla de representante: por localidad, la estación con estacion_norm alfabéticamente menor
tmp = df_estaciones[df_estaciones["codigo_localidad"] > 0].copy()
tmp = tmp.sort_values(["codigo_localidad", "estacion_norm"])
repr_por_loc = (
    tmp.groupby("codigo_localidad", as_index=False)
       .first()[["codigo_localidad", "PK_ESTACION", "estacion_norm"]]
)

# Mapeo localidad → PK_ESTACION
map_loc_to_pk = dict(zip(repr_por_loc["codigo_localidad"], repr_por_loc["PK_ESTACION"]))

# Asignación determinística de FK_ESTACION en fact
df_fact = df_fact.copy()
df_fact["FK_ESTACION"] = df_fact["codigo_localidad"].map(map_loc_to_pk).fillna(0).astype(int)

# ================== PRINTS DE CONTROL =====================
print("[CHECK] Representantes por localidad (muestra):")
print(repr_por_loc.head(10), "\n")

faltantes = sorted(set(df_fact["codigo_localidad"].unique()) - set(repr_por_loc["codigo_localidad"].unique()))
print("[CHECK] Localidades sin estación (mapeadas a FK_ESTACION=0):", faltantes, "\n")

print("[CHECK] fact con FK_ESTACION (muestra):")
print(df_fact[["codigo_localidad","FK_ESTACION"]].head(10), "\n")

print("[CHECK] Validación rápida: ¿existen FKs fuera de rango PK_ESTACION?")
print(df_fact.loc[~df_fact["FK_ESTACION"].isin(df_estaciones["PK_ESTACION"]), ["codigo_localidad","FK_ESTACION"]].head(), "\n")


# ==========================================================
# PASO 5 — Persistir dimEstacionMedicion y factFallecimientos
# ==========================================================
DIM_EST_PATH  = os.path.join(RUTA_MULTIDIM, "dimEstacionMedicion.csv")
FACT_PATH     = os.path.join(RUTA_MULTIDIM, "factFallecimientos.csv")

# Orden opcional de la dimensión por PK para lectura humana
df_estaciones = df_estaciones.sort_values("PK_ESTACION").reset_index(drop=True)

df_estaciones.to_csv(DIM_EST_PATH, index=False)
df_fact.to_csv(FACT_PATH, index=False)

print("[OK] Guardados correctamente:")
print(" -", DIM_EST_PATH)
print(" -", FACT_PATH)