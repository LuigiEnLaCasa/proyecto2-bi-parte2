# ------------------------------------------------------------
# Limpieza MortalidadPrematura2021.csv — phase1 (standard_spacetime)
# ------------------------------------------------------------
# 1) Eliminar columnas: _id, ANO, EDAD_QUINQUENAL, SUBRED.
# 2) LOCALIDAD: separar "código - nombre" → columnas:
#    codigo_localidad, nombre_localidad. Normalizar nombre:
#    minúsculas, sin tildes, trim, colapsar espacios.
# 3) Registros no útiles: casos tipo "00 - Bogotá" (no es localidad).
#    Decisión phase1: EXCLUIR de la tabla de hechos.
#    (Alternativa documentada NO aplicada: redistribuir ponderado por población).
# 4) Tipos/normalización extra: MES=int; sexo a {masculino,femenino};
#    migrante/regimen estandarizados; EDAD_FALLECIDO=int;
#    todas las columnas de texto en minúsculas y sin tildes.
# 5) Puentes para joins: year_month = f"2021-{MES:02d}";
#    localidad_std = nombre_localidad normalizado.
# ------------------------------------------------------------


import pandas as pd
import numpy as np
import unicodedata
import re

# Ruta del archivo raw
path_raw = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/raw/MortalidadPrematura2021.csv"
#print("A) tras read_csv:", pd.read_csv(path_raw, encoding="utf-8").shape)


# Cargar CSV en DataFrame
df_mortalidad = pd.read_csv(path_raw, encoding="utf-8")

# Mostrar primeras filas para verificación inicial
#print("=== HEAD INICIAL ===")
#print(df_mortalidad.head())

# Mostrar columnas antes
# print("\n=== Columnas ANTES ===")
# print(df_mortalidad.columns.tolist())

# Eliminar columnas innecesarias
df_mortalidad = df_mortalidad.drop(columns=["_id", "ANO", "EDAD_QUINQUENAL", "SUBRED"])

# Mostrar columnas después
# print("\n=== Columnas DESPUÉS ===")
# print(df_mortalidad.columns.tolist())



# ---------- Helpers de normalización ----------
def _strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return text
    return "".join(ch for ch in unicodedata.normalize("NFD", text)
                   if unicodedata.category(ch) != "Mn")

def _norm_text(text: str) -> str:
    if not isinstance(text, str):
        return text
    t = text.strip().lower()
    t = _strip_accents(t)
    t = re.sub(r"\s+", " ", t)  # colapsar espacios
    return t

# ---------- LOCALIDAD: split y normalización ----------
#print("\n=== LOCALIDAD: valores únicos (original) ===")
#print(sorted(df_mortalidad["LOCALIDAD"].dropna().unique().tolist()))

# separar por ' - ' (tolerante a espacios)
#print("Tamaño antes del split:", df_mortalidad.shape)
split_cols = df_mortalidad["LOCALIDAD"].astype(str).str.split(r"\s*-\s*", n=1, expand=True)
#print("Tamaño después del split:", split_cols.shape)
codigo_raw = split_cols[0].fillna("").str.strip()
nombre_raw = split_cols[1].fillna("").str.strip()

# si no hubo guión, usar todo como nombre y dejar código vacío
no_dash_mask = (nombre_raw == "")
nombre_raw = nombre_raw.where(~no_dash_mask, other=codigo_raw)
codigo_raw = codigo_raw.where(~no_dash_mask, other="")

# normalizar
df_mortalidad["codigo_localidad"] = codigo_raw
df_mortalidad["nombre_localidad"] = nombre_raw.map(_norm_text)

# print("\nDATAFRAME")
# print(df_mortalidad.head(10))

# print("\n=== codigo_localidad: únicos ===")
# print(sorted(df_mortalidad["codigo_localidad"].dropna().unique().tolist()))

# print("\n=== nombre_localidad (normalizado): únicos ===")
# print(sorted(df_mortalidad["nombre_localidad"].dropna().unique().tolist()))



# ------------------------------------------------------------
# Evaluación del paso 3 — distribución de valores únicos
# ------------------------------------------------------------

# Tamaño actual del DataFrame
# print("\n=== Tamaño actual del DataFrame ===")
# print(df_mortalidad.shape)

# Contar valores únicos y sus frecuencias
# print("\n=== Frecuencias codigo_localidad ===")
# print(df_mortalidad["codigo_localidad"].value_counts(dropna=False).sort_index())

# print("\n=== Frecuencias nombre_localidad ===")
# print(df_mortalidad["nombre_localidad"].value_counts(dropna=False).sort_index())

# ------------------------------------------------------------
# Paso 4 — normalización de tipos y texto
# ------------------------------------------------------------

# Convertir MES y EDAD_FALLECIDO a int
df_mortalidad["MES"] = df_mortalidad["MES"].astype(int)
df_mortalidad["EDAD_FALLECIDO"] = df_mortalidad["EDAD_FALLECIDO"].astype(int)

# Normalizar todas las columnas de texto: minúsculas, sin tildes
for col in df_mortalidad.select_dtypes(include="object").columns:
    df_mortalidad[col] = df_mortalidad[col].map(_norm_text)

# Verificar cambios
# print("\n=== Tipos de datos tras normalización ===")
# print(df_mortalidad.dtypes)

# print("\n=== Muestra tras normalización ===")
# print(df_mortalidad.head(10))


# ------------------------------------------------------------
# Paso 4.1 — distribución de localidades actuales
# ------------------------------------------------------------

# Frecuencias de las localidades normalizadas
# localidad_freq = df_mortalidad["nombre_localidad"].value_counts(dropna=False).sort_index()

# print("\n=== Distribución de localidades ===")
# print(localidad_freq)

# # Número total de registros
# print("\n=== Total de registros ===")
# print(len(df_mortalidad))

# ------------------------------------------------------------
# Paso 4.2 — eliminar registros con localidad "sin dato"
# ------------------------------------------------------------

df_mortalidad = df_mortalidad[df_mortalidad["nombre_localidad"] != "sin dato"]

# Verificar nueva distribución
# print("\n=== Distribución de localidades (sin 'sin dato') ===")
# print(df_mortalidad["nombre_localidad"].value_counts(dropna=False).sort_index())

# print("\n=== Total de registros tras eliminación ===")
# print(len(df_mortalidad))

# ------------------------------------------------------------
# Paso 4.3 (ajustado) — redistribuir "bogota" y ACTUALIZAR 3 columnas
# ------------------------------------------------------------

import numpy as np

# 1) Diccionarios base
poblacion_localidades = {
    "usaquen": 503767, "chapinero": 139701, "santa fe": 109195, "san cristobal": 402554,
    "usme": 457302, "tunjuelito": 217139, "bosa": 681234, "kennedy": 1092110,
    "fontibon": 395122, "engativa": 891530, "suba": 1124692, "barrios unidos": 243874,
    "teusaquillo": 153133, "los martires": 99423, "antonio narino": 111637,
    "puente aranda": 257242, "la candelaria": 24088, "rafael uribe uribe": 374246,
    "ciudad bolivar": 733859, "sumapaz": 6529
}
codigo_localidad_por_nombre = {
    "usaquen": "01", "chapinero": "02", "santa fe": "03", "san cristobal": "04",
    "usme": "05", "tunjuelito": "06", "bosa": "07", "kennedy": "08",
    "fontibon": "09", "engativa": "10", "suba": "11", "barrios unidos": "12",
    "teusaquillo": "13", "los martires": "14", "antonio narino": "15",
    "puente aranda": "16", "la candelaria": "17", "rafael uribe uribe": "18",
    "ciudad bolivar": "19", "sumapaz": "20"
}

# 2) Redistribuir filas "bogota" por población
mask_bogota = df_mortalidad["nombre_localidad"] == "bogota"
n_bogota = mask_bogota.sum()
localidades = np.array(list(poblacion_localidades.keys()))
p = np.array(list(poblacion_localidades.values()), dtype=float)
p = p / p.sum()
np.random.seed(42)
asignaciones = np.random.choice(localidades, size=n_bogota, p=p)

# 3) Actualizar nombre_localidad, codigo_localidad y LOCALIDAD (formato normalizado)
df_mortalidad.loc[mask_bogota, "nombre_localidad"] = asignaciones
df_mortalidad.loc[mask_bogota, "codigo_localidad"] = [
    codigo_localidad_por_nombre[n] for n in asignaciones
]
df_mortalidad.loc[mask_bogota, "LOCALIDAD"] = (
    df_mortalidad.loc[mask_bogota, "codigo_localidad"] + " - " +
    df_mortalidad.loc[mask_bogota, "nombre_localidad"]
)

# 4) (Opcional) completar códigos faltantes para filas no-bogota
faltantes = df_mortalidad["codigo_localidad"].eq("") | df_mortalidad["codigo_localidad"].isna()
df_mortalidad.loc[faltantes, "codigo_localidad"] = (
    df_mortalidad.loc[faltantes, "nombre_localidad"].map(codigo_localidad_por_nombre)
)
df_mortalidad.loc[faltantes, "LOCALIDAD"] = (
    df_mortalidad.loc[faltantes, "codigo_localidad"].fillna("") + " - " +
    df_mortalidad.loc[faltantes, "nombre_localidad"]
).str.strip(" -")

# 5) Verificación rápida
# print("\n=== Entradas únicas por columna de localidad ===")
# print(f"nombre_localidad: {df_mortalidad['nombre_localidad'].nunique()}")
# print(f"codigo_localidad: {df_mortalidad['codigo_localidad'].nunique()}")
# print(f"LOCALIDAD: {df_mortalidad['LOCALIDAD'].nunique()}")

# print("\n=== Distribución final (nombre_localidad) ===")
# print(df_mortalidad["nombre_localidad"].value_counts().sort_index())


# ------------------------------------------------------------
# Paso 4.4 — eliminar columna original LOCALIDAD
# ------------------------------------------------------------

df_mortalidad = df_mortalidad.drop(columns=["LOCALIDAD"])

# print("\n=== Columnas después de eliminar 'LOCALIDAD' ===")
# print(df_mortalidad.columns.tolist())


# ------------------------------------------------------------
# Paso final Fase 1 — guardar archivo limpio (standard_spacetime)
# ------------------------------------------------------------

out_path = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/MortalidadPrematura2021_clean.csv"
df_mortalidad.to_csv(out_path, index=False, encoding="utf-8")

print("\nArchivo guardado en fase intermedia →", out_path)
print("Tamaño final:", df_mortalidad.shape)