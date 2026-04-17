# Tarea 08: Data Engineering End-to-End — Flights Dataset

Un análisis contesta una pregunta. Un pipeline de datos la contesta todos los días, de forma confiable, sin que nadie tenga que intervenir. En esta tarea se construye ese pipeline sobre el dataset de vuelos domésticos en Estados Unidos durante 2015, pasando por una arquitectura Medallion en S3 y Athena, un modelo relacional en PostgreSQL y la base para el análisis posterior en notebook.

## Objetivo

Se trabaja sobre un dataset de vuelos de 2015 para construir un pipeline de datos end-to-end. El repositorio integra scripts ETL que implementan la arquitectura Bronze, Silver, y Gold sobre S3 y Athena con AWS Glue Data Catalog, además de un modelo relacional en PostgreSQL provisionado con CloudFormation y cargado con SQLAlchemy. Esta primera parte deja lista la infraestructura, el procesamiento y la base relacional para continuar con las consultas analíticas, visualizaciones y análisis estadístico en los siguientes pasos.

## Estructura agregada al repositorio

```text
.
├── docs
│   ├── erd-flights.drawio
│   ├── erd-flights.png
│   └── images
│       ├── 01_rds_stack.png
│       ├── 02_glue_bronze.png
│       ├── 03_glue_silver.png
│       ├── 04_glue_gold.png
│       ├── 05_athena_query.png
│       ├── 06_athena_join.png
│       ├── 07_erd.png
│       └── 08_db_counts.png
├── etl
│   ├── bronze.py
│   ├── gold.py
│   └── silver.py
├── infra
│   └── rds-flights.yaml
├── postgres
│   ├── __init__.py
│   ├── load_data.py
│   ├── models.py
│   └── setup_db.py
├── .gitignore
└── README.md
```

## Archivos principales

### etl/bronze.py

Implementa la capa Bronze. Lee los archivos CSV locales, crea la base `flights_bronze` en Glue y escribe cada tabla en S3 en su propio prefijo. También registra automáticamente el schema en el Glue Data Catalog.

### etl/silver.py

Implementa la capa Silver. Lee la tabla `flights` desde Bronze, limpia tipos, procesa los datos por chunks para evitar problemas de memoria y construye las tablas agregadas `flights_daily`, `flights_monthly` y `flights_by_airport` en formato Parquet con compresión Snappy.

### etl/gold.py

Implementa la capa Gold. Crea la base `flights_gold`, elimina la tabla analítica si ya existe y ejecuta un CTAS en Athena para construir `vuelos_analitica` a partir de las tablas de Bronze y los catálogos de aerolíneas y aeropuertos.

### infra/rds-flights.yaml

Template de CloudFormation utilizado para provisionar PostgreSQL en RDS junto con la read replica requerida para las consultas analíticas.

### postgres/models.py

Define el esquema relacional con SQLAlchemy 2.0 para las tablas `airlines`, `airports` y `flights`, incluyendo claves primarias y foráneas.

### postgres/setup_db.py

Crea las tablas en PostgreSQL utilizando el modelo definido en SQLAlchemy y el endpoint primario de RDS.

### postgres/load_data.py

Carga los archivos CSV a PostgreSQL respetando el orden correcto de inserción: `airlines`, `airports` y `flights`. Para `flights` se cargan únicamente los primeros 500,000 registros.


## Inicialización del repositorio

Se creó el repositorio público `flights-data-engineering-a`, se generó la rama `development` a partir de `main` y se trabajó sobre la rama `feature/data-engineering-flights`. Los datos se descargaron desde el bucket público del curso y se descomprimieron localmente en la carpeta `data/`. Después se agregó `data/` a `.gitignore` para evitar subir los CSVs al repositorio.


## ETL — Arquitectura Medallion

Se construyó el pipeline completo Bronze, Silver y Gold como scripts Python ejecutables desde terminal. Los scripts utilizan `logging`, `argparse`, validaciones con `assert`, manejo de errores con `try/except` y `sys.exit(1)`, además de escritura idempotente para permitir múltiples ejecuciones sin duplicar datos.

### Bronze — Ingesta sin transformaciones

En Bronze se cargaron los tres archivos del dataset a S3 sin transformaciones, preservando la fuente original de verdad. Antes de escribir las tablas, se creó la base `flights_bronze` en Glue con `exist_ok=True`. Cada tabla se escribió en su propio prefijo bajo `flights/bronze/` y quedó registrada automáticamente en el Glue Data Catalog.

La evidencia de esta capa muestra que `flights_bronze` contiene correctamente las tres tablas requeridas: `airlines`, `airports` y `flights`.

![Bronze](docs/images/02_glue_bronze.png)

### Silver — Transformaciones y agregaciones

En Silver se transformaron los datos de Bronze a formato Parquet + Snappy y se construyeron tres tablas agregadas: `flights_daily`, `flights_monthly` y `flights_by_airport`. Para evitar errores de memoria durante la lectura de `flights.csv`, el procesamiento se hizo por chunks. Antes de escribir cada tabla, se validó que el DataFrame no estuviera vacío y que contuviera las columnas esperadas.

La tabla `flights_daily` quedó particionada por `MONTH`, mientras que `flights_monthly` y `flights_by_airport` se escribieron en modo overwrite. Después se validó también en Athena que las agregaciones de Silver respondieran correctamente.

![Silver](docs/images/03_glue_silver.png)

![Validación Silver en Athena](docs/images/05_athena_query.png)

### Gold — Tabla analítica

En Gold se creó la base `flights_gold` y se construyó la tabla `vuelos_analitica` con un CTAS en Athena. Esta tabla desnormaliza los datos de vuelos y los enriquece con los catálogos de aerolíneas y aeropuertos, resolviendo nombres descriptivos para origen, destino y aerolínea.

La validación final se realizó con una consulta de muestra para comprobar que los nombres de aerolínea y aeropuerto quedaran correctamente registrados.

![Gold](docs/images/04_glue_gold.png)

![Validación Gold en Athena](docs/images/06_athena_join.png)


## Diagrama Entidad-Relación (ERD) y PostgreSQL

En esta parte se modeló el esquema relacional del dataset, se provisionó PostgreSQL en AWS y se cargaron los datos requeridos para las consultas de los pasos posteriores.

### Provisionamiento con CloudFormation

Se copió el template del demo de clase a `infra/rds-flights.yaml`, se ajustó el parámetro `DBName` a `flights` y se desplegó el stack `itam-flights-rds` desde la consola de CloudFormation. El stack se creó con read replica, y se verificaron los outputs con el endpoint primario y el endpoint de la réplica.

![CloudFormation Outputs](docs/images/01_rds_stack.png)

### Diseño del ERD

Se construyó el diagrama entidad-relación en draw.io con las tres entidades solicitadas: `airlines`, `airports` y `flights`. El modelo incluye claves primarias, claves foráneas y la doble referencia de `flights` hacia `airports` para origen y destino.

Además del screenshot, se guardó también el archivo fuente `docs/erd-flights.drawio` y la exportación como imagen en `docs/erd-flights.png`.

![ERD](docs/images/07_erd.png)

### Creación de tablas con SQLAlchemy

Se definió el modelo con SQLAlchemy 2.0 en `postgres/models.py`, respetando el ERD anterior. Luego, mediante `postgres/setup_db.py`, se conectó al endpoint primario de RDS y se crearon las tablas `airlines`, `airports` y `flights` en PostgreSQL.

### Carga de datos en PostgreSQL

Los datos se cargaron con el patrón de bulk insert de SQLAlchemy 2.0. El orden de inserción respetó las dependencias entre tablas: primero `airlines`, después `airports` y finalmente `flights`. Para `flights` se cargaron únicamente los primeros 500,000 registros, tal como lo especificaba la tarea.

La carga se validó en DBeaver mediante un `SELECT COUNT(*)` por tabla. Los resultados finales fueron 14 registros en `airlines`, 322 registros en `airports` y 500,000 registros en `flights`.

![Conteo de tablas en PostgreSQL](docs/images/08_db_counts.png)

## 6. Conecta con DBeaver y responde las preguntas con SQL

En esta parte del proyecto se utilizó **DBeaver Community Edition** conectado a la **Read Replica** de PostgreSQL. La instancia primaria se reservó para crear el schema e insertar datos, mientras que las consultas analíticas se ejecutaron sobre la réplica para no afectar el rendimiento de escritura.

### 6.1 Conexión a la Read Replica

Parámetros de conexión utilizados:

- **Host**: `RdsReplicaEndpoint`
- **Port**: `5432`
- **Database**: `flights`
- **Username**: `itam`
- **Password**: la contraseña definida al crear el stack

### Evidencia de conexión

**Añadir imagen aquí:**

```markdown
![Conexión exitosa a la Read Replica en DBeaver](docs/screenshots/dbeaver_connection.jpeg)
```


---

### 6.2 Preguntas con `SELECT`

Se resolvieron las siguientes consultas en DBeaver:

- **P1.** ¿Cuáles son las 10 rutas (origen → destino) con mayor número de vuelos?
- **P2.** ¿Cuáles son las 5 aerolíneas con mayor porcentaje de vuelos cancelados?
- **P3.** ¿Cuántos vuelos fueron cancelados por cada causa (`CANCELLATION_REASON`)?
- **P4.** ¿Cuál es el retraso promedio de salida por mes, considerando únicamente vuelos realmente retrasados?
- **P5.** ¿Cuáles son los 10 aeropuertos de origen con más minutos totales de retraso atribuidos al clima (`WEATHER_DELAY`)?

#### Evidencias

##### P1. Top 10 rutas con mayor número de vuelos

**Añadir imagen aquí:**

```markdown
![P1 - Top 10 rutas con mayor número de vuelos](docs/screenshots/p1_top_routes.jpeg)
```

##### P2. Top 5 aerolíneas con mayor porcentaje de cancelación

**Añadir imagen aquí:**

```markdown
![P2 - Top 5 aerolíneas con mayor porcentaje de cancelación](docs/screenshots/p2_cancel_p.png)
```

##### P3. Conteo de vuelos cancelados por causa

**Añadir imagen aquí:**

```markdown
![P3 - Conteo de vuelos cancelados por causa](docs/screenshots/p3_cancel_reason.png)
```

##### P4. Retraso promedio de salida por mes

**Añadir imagen aquí:**

```markdown
![P4 - Retraso promedio de salida por mes](docs/screenshots/p4_avg_dep_delay.png)
```

##### P5. Aeropuertos con más minutos de weather delay

**Añadir imagen aquí:**

```markdown
![P5 - Aeropuertos con más minutos de weather delay](docs/screenshots/p5_weather_delay.png)
```

---

### 6.3 Preguntas con Window Functions

Se resolvieron las siguientes consultas:

- **W1.** Para cada aerolínea, ¿cuál fue el vuelo con el mayor retraso de llegada?
- **W2.** ¿Cuál fue la variación mes a mes en el total de vuelos durante 2015?
- **W3.** Para el aeropuerto `LAX` el día `2015-01-01`, ¿cuáles fueron los primeros 5 vuelos según el horario de salida programado?

#### Nota sobre W2

La pregunta **W2 no se resolvió en PostgreSQL**, sino en **Athena** sobre `flights_silver.flights_monthly`, porque la carga de 500,000 filas en PostgreSQL no cubre de forma representativa todo el año 2015. Por lo tanto, W2 se documenta en el notebook del paso 7.

#### Evidencias

##### W1. Mayor retraso de llegada por aerolínea

**Añadir imagen aquí:**

```markdown
![W1 - Mayor retraso de llegada por aerolínea](docs/screenshots/w1_max_arrival_delay.png)
```

##### W2. Variación mes a mes del total de vuelos (Athena)

**Añadir imagen aquí:**

```markdown
![W2 - Variación mes a mes del total de vuelos](docs/screenshots/w2_athena.png)
```

##### W3. Primeros 5 vuelos de LAX el 2015-01-01

**Añadir imagen aquí:**

```markdown
![W3 - Primeros 5 vuelos de LAX el 2015-01-01](docs/screenshots/w3_lax_first5.png)
```

---

## 7. Repite las consultas en un Jupyter Notebook con `awswrangler`

El notebook principal del proyecto es:

```text
notebooks/flights_analytics.ipynb
```

Este notebook integra dos fuentes:

1. **PostgreSQL Read Replica**, para ejecutar **P1–P5, W1 y W3**
2. **Athena**, para ejecutar **W2**, así como los análisis de regresión y pronóstico

### Qué hace el notebook

- ejecuta las 8 preguntas como queries SQL
- muestra los resultados como DataFrames
- agrega visualizaciones para comunicar mejor cada respuesta
- integra la parte analítica de regresión lineal y series de tiempo en el mismo flujo

### Visualizaciones destacadas del notebook

##### Notebook — P1

**Añadir imagen aquí:**

```markdown
![Notebook P1](docs/screenshots/notebook_p1.png)
```

##### Notebook — P2

**Añadir imagen aquí:**

```markdown
![Notebook P2](docs/screenshots/notebook_p2.png)
```

##### Notebook — W2 (Athena)

**Añadir imagen aquí:**

```markdown
![Notebook W2 - Variación mes a mes del total de vuelos](docs/screenshots/notebook_w2.png)
```

---

## 8. Análisis Estadístico

Además de las consultas SQL, se incorporó una sección de análisis estadístico dentro del mismo notebook `flights_analytics.ipynb`. Esta parte se divide en dos bloques:

1. **Regresión lineal con `statsmodels`**
2. **Pronóstico de series de tiempo con `StatsForecast`**

---

### 8.1 Regresión lineal — ¿Qué factores explican el retraso de llegada?

#### Objetivo

El objetivo de esta sección fue estudiar **qué variables están más asociadas con el retraso total de llegada** (`arrival_delay`). No se trató de construir un modelo predictivo de producción, sino de usar una regresión lineal como herramienta de interpretación estadística.

#### Fuente de datos

Los datos se leyeron desde la capa Gold en Athena, usando la tabla:

```text
flights_gold.vuelos_analitica
```

#### Variable objetivo

- `y = arrival_delay`

#### Variables explicativas

- `departure_delay`
- `distance`
- `air_system_delay`
- `airline_delay`
- `weather_delay`
- `late_aircraft_delay`
- `security_delay`

#### Modelo usado

Se ajustó un modelo **OLS** con `statsmodels`, ya que esta librería permite obtener:

- coeficientes
- intervalos de confianza
- p-values
- métricas de ajuste
- diagnósticos de residuos

#### Métricas reportadas

- **R²**
- **RMSE**

#### Visualizaciones requeridas

##### 1. Coeficientes con intervalos de confianza

Esta gráfica permite comparar el peso relativo de cada variable explicativa y observar la incertidumbre asociada a cada coeficiente.

```markdown
![Regresión OLS - Coeficientes con intervalos de confianza](docs/screenshots/regression_coefficients.png)
```

##### 2. Valores predichos vs. valores reales

Este gráfico permite evaluar qué tan alineados están los valores ajustados por el modelo respecto a los valores observados.

```markdown
![Regresión OLS - Predichos vs reales](docs/screenshots/regression_pred_vs_real.png)
```

##### 3. Residuos vs. valores predichos

Se utilizó para revisar si existe algún patrón sistemático en los residuos y evaluar si la especificación lineal parece razonable.

```markdown
![Regresión OLS - Residuos vs predichos](docs/screenshots/regression_residuals.png)
```

##### 4. Q-Q plot de residuos

Este gráfico compara la distribución empírica de los residuos contra una normal teórica, permitiendo inspeccionar desviaciones en colas.

**Añadir imagen aquí:**

```markdown
![Regresión OLS - Q-Q plot de residuos](docs/screenshots/regression_qqplot.png)
```

#### Interpretación

En esta sección se reportan **R²** y **RMSE**, además de una interpretación breve de los diagnósticos. Dado que varios componentes del retraso están altamente relacionados con el retraso total, era esperable observar **multicolinealidad**, por lo que los coeficientes deben interpretarse con cautela. Aun así, el modelo permite identificar qué variables tienen mayor asociación con el retraso de llegada y qué tan bien se ajusta una especificación lineal simple a este problema.

---

### 8.2 Pronóstico de series de tiempo — ¿Cuántos vuelos habrá en los próximos meses?

#### Objetivo

El objetivo de esta sección fue construir una serie mensual del total de vuelos durante 2015 y comparar varios modelos automáticos de pronóstico para proyectar los meses siguientes.

#### Fuente de datos

Se utilizó la tabla:

```text
flights_silver.flights_monthly
```

A partir de ella se construyó una serie en formato largo con las columnas requeridas por `StatsForecast`:

- `unique_id`
- `ds`
- `y`

#### Modelos comparados

- `AutoETS`
- `AutoARIMA`
- `AutoTheta`

#### Configuración del ejercicio

- frecuencia mensual
- `season_length = 12`
- train: enero–septiembre 2015
- test: octubre–diciembre 2015
- horizonte total: 9 pasos
  - 3 pasos para evaluar sobre test
  - 6 pasos hacia adelante
- intervalos de confianza: **90%**

#### Métrica reportada

- **MAE** sobre el test set

#### Qué se reporta

- modelo seleccionado automáticamente por cada algoritmo
- MAE de cada modelo
- interpretación breve de desempeño e incertidumbre

#### Visualizaciones requeridas

##### 1. Evaluación sobre el test set

Se grafican:
- los 9 meses de entrenamiento
- los 3 meses del test set reales
- el pronóstico de cada modelo sobre esos 3 meses
- las bandas de confianza al 90%

**Añadir imagen aquí:**

```markdown
![Forecast - Evaluación sobre test set](docs/screenshots/forecast_test_eval.png)
```

##### 2. Pronóstico de 6 meses hacia adelante

Se muestra la serie observada de 2015 junto con el pronóstico de los siguientes 6 meses para los tres modelos.

**Añadir imagen aquí:**

```markdown
![Forecast - Pronóstico de 6 meses hacia adelante](docs/screenshots/forecast_6m.png)
```

##### 3. Comparación de MAE

Se resume el error absoluto medio de cada modelo sobre el test set, lo que permite comparar cuál se desempeñó mejor en la evaluación fuera de muestra.

**Añadir imagen aquí:**

```markdown
![Forecast - Comparación de MAE](docs/screenshots/forecast_mae.png)
```

#### Interpretación

Con solo 12 meses de historia y 9 meses de entrenamiento, la incertidumbre del pronóstico es alta, por lo que las bandas de confianza son una parte central del análisis. La comparación por **MAE** permite identificar cuál de los tres modelos se acercó mejor a los valores reales del test set, mientras que las gráficas ayudan a evaluar visualmente estabilidad y amplitud de los intervalos.

---

## Archivos clave usados en esta sección

- `notebooks/flights_analytics.ipynb`
- `docs/screenshots/`
- `postgres/models.py`
- `postgres/setup_db.py`
- `postgres/load_data.py`

---

## Nota final

Esta parte del proyecto complementa el pipeline Bronze → Silver → Gold con una capa de consumo analítico orientada a exploración, interpretación y pronóstico:

- SQL exploratorio y validación en DBeaver
- consultas repetibles en notebook
- interpretación estadística con OLS
- pronóstico mensual con modelos automáticos de series de tiempo
