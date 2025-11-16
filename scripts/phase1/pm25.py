# ------------------------------------------------------------
# Limpieza PM25_2021.csv — phase1 (standard_spacetime)
# ------------------------------------------------------------
# 1) Tiempo mensual:
#    - Parsear fecha_hora y derivar year_month = YYYY-MM (ej. 2021-01).
#    - Descartar día y hora; mantener solo la granularidad mensual.
#
# 2) Localidad desde estación:
#    - Mapear columna 'estacion' → (codigo_localidad, nombre_localidad)
#      usando un diccionario estaciones→localidad.
#    - Normalizar nombre_localidad: minúsculas, sin tildes, trim.
#
# 3) Agregación mensual:
#    - Convertir medidas horarias/diarias a nivel mensual por localidad.
#    - Agregación: promedio mensual por (year_month, localidad) de
#      pm25_concentracion, pm25_media_movil, iboca.
#    - Si hay varias estaciones por la misma localidad en un mes,
#      promediar entre estaciones.
#
# 4) Tipos y nombres:
#    - Cast numéricos a float.
#    - Strings en minúsculas y sin tildes.
#    - Renombrar columnas a snake_case consistente si aplica
#      (ej.: iboca → pm25_iboca; pm25_media_movil se mantiene).
#
# 5) Columnas finales (phase1):
#    year_month, codigo_localidad, nombre_localidad,
#    pm25_concentracion, pm25_media_movil, pm25_iboca
#
# 6) Salida:
#    - Guardar resultado mensual por localidad en:
#      /Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime
# ------------------------------------------------------------



# ------------------------------------------------------------
# PM25_2021.csv — phase1 (standard_spacetime)
# Paso 1: importación de librerías y lectura del CSV
# ------------------------------------------------------------

import pandas as pd
import numpy as np

# Ruta del archivo raw
path_raw = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/raw/PM25_2021.csv"

# Cargar CSV en DataFrame
df_pm25 = pd.read_csv(path_raw, encoding="utf-8")

# Mostrar primeras filas para verificación inicial
# print("=== HEAD INICIAL ===")
# print(df_pm25.head())


# ------------------------------------------------------------
# Paso 2 — explorar valores únicos de la columna 'estacion'
# ------------------------------------------------------------

# print("\n=== Entradas únicas en 'estacion' ===")
# print(sorted(df_pm25["estacion"].dropna().unique().tolist()))
# print("\nTotal de estaciones únicas:", df_pm25["estacion"].nunique())

# ------------------------------------------------------------
# Paso 2.1 — corrección de nombres mal codificados en 'estacion'
# ------------------------------------------------------------

# Diccionario de reemplazos por errores de encoding
reemplazos_estaciones = {
    "Ciudad BolГ\xadvar": "Ciudad Bolivar",
    "JazmГ\xadn": "Jazmín",
    "MГіvil 7ma": "Móvil 7ma"
}

# Aplicar reemplazos
df_pm25["estacion"] = df_pm25["estacion"].replace(reemplazos_estaciones)

# Verificar corrección
# print("\n=== Valores únicos corregidos en 'estacion' ===")
# print(sorted(df_pm25["estacion"].dropna().unique().tolist()))
# print("\nTotal de estaciones únicas:", df_pm25["estacion"].nunique())


direccion_por_estacion = {
    "guaymaral": "autopista norte # 205-59",
    "usaquen": "carrera 7b bis # 132-11",
    "suba": "carrera 111 # 159a-61",
    "bolivia": "avenida calle 80 # 121-98",
    "las ferias": "avenida calle 80 # 69q-50",
    "cdar": "calle 63 # 59a-06",
    "movil 7ma": "carrera 7 con calle 60",
    "minambiente": "calle 37 # 8-40",
    "fontibon": "carrera 104 # 20 c - 31",
    "puente aranda": "calle 10 # 65-28",
    "kennedy": "carrera 80 # 40-55 sur",
    "carvajal - sevillana": "autopista sur # 63-40",
    "tunal": "carrera 24 # 49-86 sur",
    "san cristobal": "carrera 2 este # 12-78 sur",
    "jazmin": "calle 1 g # 41 a 39",
    "usme": "carrera 11 # 65 d 50 sur",
    "bosa": "diagonal 73 f sur # 78 - 44",
    "ciudad bolivar": "calle 70 sur # 56 - 11",
    "colina": "avenida boyaca no 142e-55",
    "movil fontibon": "cra. 98 # 16 b 50",
}

localidad_por_estacion = {
    "guaymaral": "suba",
    "usaquen": "usaquen",
    "suba": "suba",
    "bolivia": "engativa",
    "las ferias": "engativa",
    "cdar": "engativa",
    "movil 7ma": "chapinero",
    "minambiente": "santa fe",
    "fontibon": "fontibon",
    "puente aranda": "puente aranda",
    "kennedy": "kennedy",
    "carvajal - sevillana": "kennedy",
    "tunal": "tunjuelito",
    "san cristobal": "san cristobal",
    "jazmin": "puente aranda",
    "usme": "usme",
    "bosa": "bosa",
    "ciudad bolivar": "ciudad bolivar",
    "colina": "suba",
    "movil fontibon": "fontibon",
}

# ------------------------------------------------------------
# Paso 3 — verificación de coincidencia entre estaciones y diccionarios
# ------------------------------------------------------------

import unicodedata
import re

# --- Helper para normalizar texto ---
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
    t = re.sub(r"\s+", " ", t)
    return t

# Normalizar columna 'estacion'
df_pm25["estacion_norm"] = df_pm25["estacion"].map(_norm_text)

# Obtener listas únicas normalizadas
estaciones_csv = sorted(df_pm25["estacion_norm"].dropna().unique().tolist())
llaves_dicc = sorted(list(localidad_por_estacion.keys()))

# Comparación
coincidentes = [e for e in estaciones_csv if e in llaves_dicc]
no_coincidentes = [e for e in estaciones_csv if e not in llaves_dicc]

# print("\n=== Estaciones en CSV (normalizadas) ===")
# print(estaciones_csv)
# print("\n=== Llaves de diccionarios ===")
# print(llaves_dicc)

# print("\n=== Coinciden ===")
# print(coincidentes)

# print("\n=== No coinciden ===")
# print(no_coincidentes)


# ------------------------------------------------------------
# Paso 4 — añadir Dirección y Localidad desde diccionarios
# ------------------------------------------------------------

# Asegúrate de tener df_pm25["estacion_norm"] creado con _norm_text
df_pm25["direccion"] = df_pm25["estacion_norm"].map(direccion_por_estacion)
df_pm25["nombre_localidad"] = df_pm25["estacion_norm"].map(localidad_por_estacion)

# Diagnóstico de mapeos faltantes
faltan_dir = df_pm25["direccion"].isna().sum()
faltan_loc = df_pm25["nombre_localidad"].isna().sum()
#print(f"Mapeos faltantes → direccion: {faltan_dir}, localidad: {faltan_loc}")

# Opcional: mostrar estaciones no mapeadas
# if faltan_dir or faltan_loc:
#     no_map = df_pm25.loc[df_pm25["direccion"].isna() | df_pm25["nombre_localidad"].isna(),
#                          ["estacion", "estacion_norm"]].drop_duplicates()
#     print("\nEstaciones no mapeadas:")
#     print(no_map)

# Mostrar primeras filas para verificación
# print("\n=== HEAD con columnas nuevas ===")
# print(df_pm25.head())

# Mostrar valores únicos de nombre_localidad
localidades_unicas = sorted(df_pm25["nombre_localidad"].dropna().unique().tolist())
# print("\n=== Localidades únicas en 'nombre_localidad' ===")
# print(localidades_unicas)
# print("\nTotal de localidades únicas:", len(localidades_unicas))


# ------------------------------------------------------------
# Paso 5 — añadir Sigla, Latitud y Longitud según estación
# ------------------------------------------------------------

info_estaciones = {
    "guaymaral": {"sigla": "GYR", "lat": "4°47'01.5\"N", "lon": "74°02'38.9\"W"},
    "usaquen": {"sigla": "USQ", "lat": "4°42'37.26\"N", "lon": "74°14'9.50\"W"},
    "suba": {"sigla": "SUB", "lat": "4°45'40.49\"N", "lon": "74°5'36.46\"W"},
    "bolivia": {"sigla": "BOL", "lat": "4°44'08.9\"N", "lon": "74°07'33.2\"W"},
    "las ferias": {"sigla": "LFR", "lat": "4°41'26.52\"N", "lon": "74°4'56.94\"W"},
    "cdar": {"sigla": "CDAR", "lat": "4°39'30.48\"N", "lon": "74°5'2.28\"W"},
    "movil 7ma": {"sigla": "MOV1", "lat": "4°38'42.7\"N", "lon": "74°03'41.6\"W"},
    "minambiente": {"sigla": "MAM", "lat": "4°37'31.75\"N", "lon": "74°4'1.13\"W"},
    "fontibon": {"sigla": "FTB", "lat": "4°40'41.67\"N", "lon": "74°8'37.75\"W"},
    "puente aranda": {"sigla": "PTE", "lat": "4°37'54.36\"N", "lon": "74°7'2.94\"W"},
    "kennedy": {"sigla": "KEN", "lat": "4°37'30.18\"N", "lon": "74°9'40.80\"W"},
    "carvajal - sevillana": {"sigla": "CSE", "lat": "4°35'45.0\"N", "lon": "74°08'54.6\"W"},
    "tunal": {"sigla": "TUN", "lat": "4°34'34.41\"N", "lon": "74°7'51.44\"W"},
    "san cristobal": {"sigla": "SCR", "lat": "4°34'21.19\"N", "lon": "74°5'1.73\"W"},
    "jazmin": {"sigla": "JAZ", "lat": "4°36'30.6\"N", "lon": "74°06'53.8\"W"},
    "usme": {"sigla": "USM", "lat": "4°31'55.4\"N", "lon": "74°7'01.7\"W"},
    "bosa": {"sigla": "BOS", "lat": "4°36'20.2\"N", "lon": "74°12'14.6\"W"},
    "ciudad bolivar": {"sigla": "CBV", "lat": "4°34'40.1\"N", "lon": "74°09'58.6\"W"},
    "colina": {"sigla": "COL", "lat": "4°44'13.9\"N", "lon": "74°04'10.1\"W"},
    "movil fontibon": {"sigla": "MOV2", "lat": "4°40'04.8\"N", "lon": "74°08'54.6\"W"},
}

# Añadir columnas al DataFrame
df_pm25["sigla"] = df_pm25["estacion_norm"].map(lambda x: info_estaciones.get(x, {}).get("sigla"))
df_pm25["latitud"] = df_pm25["estacion_norm"].map(lambda x: info_estaciones.get(x, {}).get("lat"))
df_pm25["longitud"] = df_pm25["estacion_norm"].map(lambda x: info_estaciones.get(x, {}).get("lon"))

# # Verificar
# print("\n=== HEAD con columnas sigla, latitud y longitud ===")
# print(df_pm25[["estacion", "sigla", "latitud", "longitud", "nombre_localidad", "direccion"]].head())


# ------------------------------------------------------------
# Verificar entradas específicas de la estación MOV1
# ------------------------------------------------------------

# print("\n=== Entradas correspondientes a la estación MOV1 ===")
# print(df_pm25[df_pm25["sigla"] == "MOV1"].head(10))


# ------------------------------------------------------------
# Paso 6 — añadir código_localidad (según división territorial)
# ------------------------------------------------------------

codigo_localidad_por_nombre = {
    "usaquen": "01",
    "chapinero": "02",
    "santa fe": "03",
    "san cristobal": "04",
    "usme": "05",
    "tunjuelito": "06",
    "bosa": "07",
    "kennedy": "08",
    "fontibon": "09",
    "engativa": "10",
    "suba": "11",
    "barrios unidos": "12",
    "teusaquillo": "13",
    "los martires": "14",
    "antonio narino": "15",
    "puente aranda": "16",
    "la candelaria": "17",
    "rafael uribe uribe": "18",
    "ciudad bolivar": "19",
    "sumapaz": "20",
}

# Crear columna 'codigo_localidad'
df_pm25["codigo_localidad"] = df_pm25["nombre_localidad"].map(codigo_localidad_por_nombre)

# Verificar si hubo localidades sin código
faltan_codigos = df_pm25["codigo_localidad"].isna().sum()
# print(f"\nFaltan códigos para {faltan_codigos} registros")

# Mostrar resumen de distribución de localidades
# print("\n=== Distribución de nombre_localidad y sus códigos ===")
# print(df_pm25.groupby(["nombre_localidad", "codigo_localidad"]).size())


# ------------------------------------------------------------
# Paso 7 — conversión de columnas numéricas a float
# ------------------------------------------------------------

cols_float = ["pm25_concentracion", "pm25_media_movil", "iboca"]

for col in cols_float:
    # Reemplazar comas por puntos y convertir a float
    df_pm25[col] = (
        df_pm25[col]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

# Verificar tipos finales
# print("\n=== Tipos de datos después de conversión ===")
# print(df_pm25[cols_float].dtypes)

# Verificar muestra
# print("\n=== HEAD de columnas numéricas convertidas ===")
# print(df_pm25[cols_float].head())

# ------------------------------------------------------------
# Paso final Fase 1 — guardar archivo limpio (standard_spacetime)
# ------------------------------------------------------------

out_path = "/Users/luchopinilla/Desktop/BI/BRUTALISMUS/data/standard_spacetime/PM25_2021_clean.csv"
df_pm25.to_csv(out_path, index=False, encoding="utf-8")

print("\nArchivo guardado en fase intermedia →", out_path)
print("Tamaño final:", df_pm25.shape)