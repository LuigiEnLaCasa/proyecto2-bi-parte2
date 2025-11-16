# Phase 1 — Transformaciones aplicadas a los datasets base

En la fase 1 se limpian y estandarizan todas las fuentes crudas para generar datasets coherentes con formato temporal uniforme (year_month) y nombres normalizados. Los archivos resultantes se guardan en data/standard_spacetime/.

---

## mortalidad.py
Limpieza de MortalidadPrematura2021.csv.
Transformaciones aplicadas:
- Eliminar columnas: _id, ANO, EDAD_QUINQUENAL, SUBRED.
- Separar columna LOCALIDAD "codigo - nombre" en:
  codigo_localidad, nombre_localidad.
- Normalizar nombre_localidad:
  minusculas, sin tildes, trim, colapsar espacios.
- Excluir registros donde la localidad es "00 - Bogota" (no es localidad valida).
- Normalizar texto en todas las columnas:
  minusculas, sin tildes, estandarizar migrante/regimen/sexo.
- Convertir MES a entero.
- Convertir EDAD_FALLECIDO a entero.
- Crear columna year_month = "2021-{MES:02d}".
- Crear localidad_std como version normalizada del nombre de localidad.
- Guardar mortalidad_2021_clean.csv en standard_spacetime.

---

## pm25.py
Limpieza de PM25_2021.csv.
Transformaciones aplicadas:
- Parsear fecha_hora → derivar year_month = YYYY-MM.
- Descartar dia y hora; conservar granularidad mensual.
- Mapear estacion → (codigo_localidad, nombre_localidad) con diccionarios internos.
- Normalizar nombre_localidad:
  minusculas, sin tildes, trim.
- Convertir mediciones horarias/diarias a nivel mensual:
  promedio mensual por (year_month, localidad) de:
  pm25_concentracion, pm25_media_movil, iboca.
- Crear columnas year_month, codigo_localidad, nombre_localidad.
- Verificar que todas las estaciones existan en los diccionarios.
- Guardar pm25_2021_clean.csv en standard_spacetime.

---

## precipitacion.py
Limpieza de precipitacion_2021.csv.
Transformaciones aplicadas:
- Parsear fecha_mes → derivar year_month = YYYY-MM.
- Conservar solo el mes; eliminar granularidad diaria si existe.
- Convertir precipitacion_mm a float.
- Renombrar columnas a snake_case.
- Mantener columnas finales:
  year_month, precipitacion_mm.
- Guardar precipitacion_2021_clean.csv en standard_spacetime.

---

## temperatura.py
Limpieza de temperatura_2021.csv.
Transformaciones aplicadas:
- Convertir fecha y hora originales a datetime unificado.
- Derivar year_month = YYYY-MM.
- Convertir temperatura_valor a tipo numerico.
- Normalizar nombres de columnas a snake_case.
- Eliminar duplicados.
- Seleccionar y ordenar columnas finales relevantes para la fase 2.
- Guardar temperatura_2021_clean.csv en standard_spacetime.

# Phase 2 — Integracion y enriquecimiento del modelo espacial, temporal y ambiental

En la fase 2 se integran los datos limpios de fase 1 para construir datasets que relacionan mortalidad con ambiente, contaminacion, temperatura, precipitacion y cobertura espacial. Se crean salidas intermedias con granularidad mensual y por localidad.

---

## contar_loc_sin_medicion.py
Objetivo: identificar localidades que no tienen mediciones ambientales disponibles.
Transformaciones aplicadas:
- Cargar datasets limpios de PM25, precipitacion y temperatura desde standard_spacetime.
- Extraer el conjunto de localidades que aparecen en mortalidad.
- Extraer localidades que aparecen en cada una de las fuentes ambientales.
- Comparar y calcular:
  - localidades sin PM25,
  - localidades sin precipitacion,
  - localidades sin temperatura.
- Generar tablas resumen con el listado de localidades faltantes por cada categoria.
- Guardar archivos CSV de localidades sin medicion en la carpeta phase2_output.

---

## mortalidad_ambiente.py
Objetivo: unir mortalidad mensual con variables ambientales agregadas por mes y localidad.
Transformaciones aplicadas:
- Cargar mortalidad_2021_clean.csv.
- Cargar datasets de precipitacion y temperatura ya agregados por year_month.
- Realizar merge por year_month y localidad:
  mortalidad + precipitacion.
  mortalidad + temperatura.
- Mantener columnas clave:
  year_month, codigo_localidad, nombre_localidad, precipitacion_mm, temperatura_valor.
- Agregar indicadores en caso de valores faltantes para diagnostico.
- Guardar mortalidad_ambiente_2021.csv en phase2_output.

---

## mortalidad_contaminacion.py
Objetivo: integrar mortalidad con las mediciones de PM25.
Transformaciones aplicadas:
- Cargar mortalidad_2021_clean.csv y pm25_2021_clean.csv.
- Normalizar nombre_localidad para garantizar coincidencia exacta.
- Realizar merge por year_month y codigo_localidad.
- Mantener columnas clave:
  pm25_concentracion, pm25_media_movil, iboca.
- Crear flags de filas sin datos de PM25 para auditoria.
- Guardar mortalidad_contaminacion_2021.csv en phase2_output.

---

## mortalidad_punto.py
Objetivo: integrar mortalidad con la informacion geoespacial de puntos de medicion (estaciones).
Transformaciones aplicadas:
- Cargar mortalidad_2021_clean.csv.
- Cargar dataset de estaciones con coordenadas y localidad asignada.
- Normalizar nombres de localidad para asegurar match.
- Unir mortalidad con estaciones segun localidad.
- Generar tabla final donde cada registro de mortalidad queda vinculado a:
  estacion, latitud, longitud, direccion_estacion.
- Guardar mortalidad_punto_2021.csv en phase2_output.

---

## mortalidad_tiempo.py
Objetivo: generar una tabla temporal limpia para modelado por mes.
Transformaciones aplicadas:
- Cargar mortalidad_2021_clean.csv.
- Extraer columna year_month.
- Eliminar duplicados para obtener lista unica de meses presentes en el dataset.
- Ordenar por mes.
- Exportar dimension temporal:
  year_month, year, month.
- Guardar dim_tiempo_2021.csv en phase2_output.

# Phase 3 — Depuracion final de la tabla de hechos factFallecimientos

En la fase 3 se depura y reordena la tabla de hechos `factFallecimientos` ya generada en el modelo multidimensional, eliminando columnas redundantes y colocando las llaves foraneas al inicio.

---

## cleanse_factFallecimientos.py

Objetivo:
- Eliminar columnas redundantes (ya modeladas en dimensiones).
- Eliminar `CIE10_AGRUPADA` (solo tiene un valor).
- Reordenar columnas para dejar primero las FKs.
- Sobrescribir el CSV final depurado.

Transformaciones aplicadas:

1. Carga del dataset
   - Carga `factFallecimientos.csv` desde `data/multidimensional/`.
   - Muestra forma inicial y listado de columnas.

2. Eliminacion de columnas redundantes o temporales  
   Se eliminan, si existen, las columnas:
   - `MES`
   - `DIA_FALLECIMIENTO`
   - `HORA_FALLECIMIENTO`
   - `FECHA_HORA`
   - `nombre_localidad`
   - `codigo_localidad`
   - `CIE10_AGRUPADA`
   - Se imprime listado de columnas efectivamente eliminadas.
   - Se muestra la nueva forma del dataframe y las columnas restantes.

3. Reordenamiento de columnas (FKs primero)
   - Se detectan como candidatas a FK (solo si existen):
     - `FK_TIEMPO`
     - `FK_ESTACION`
     - `FK_CONTAMINACION`
     - `FK_ambiente`
     - `FK_AMBIENTE`
   - Se construye un nuevo orden de columnas:
     - primero las FKs detectadas,
     - luego el resto de columnas en el orden original.
   - Se reindexa el dataframe con ese nuevo orden.
   - Se imprimen las FKs detectadas y las primeras columnas resultantes.

4. Validacion de `CIE10_BASICA`
   - Si la columna `CIE10_BASICA` existe:
     - Se listan sus valores unicos (sin nulos).
     - Se imprime el total de codigos unicos.

5. Persistencia del CSV final
   - Se sobrescribe `factFallecimientos.csv` en la misma ruta, sin indice.
   - Se imprime mensaje de exito con la ubicacion del archivo.
