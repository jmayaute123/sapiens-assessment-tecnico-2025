# sapiens-assessment

Assessment Técnico – Data Engineer GCP

## Introducción

Este proyecto, que es un pipeline desarrollado en GCP, tiene como objetivo implementar y optimizar pipelines de datos, procesar datos a gran escala con BigQuery, Cloud Functions y Pub/Sub, y demostrar dominio en SQL avanzado, Python y buenas prácticas de ingeniería de datos.

## Diagrama de Arquitectura

![Sapiens_Diagrama_Arquitectura](https://github.com/user-attachments/assets/dab9cbea-fd54-42d0-915d-9f6921e13bde)

## Justificación de Servicios Utilizados en el Diagrama

1. **Cloud Storage:**
Se eligió este servicio como punto de entrada para almacenar las fuentes de datos 'ventas.csv' y 'eventos_navegación.json' debido a su escalabilidad, bajo costo y compatibilidad con múltiples formatos. En este proyecto, Cloud Storage, facilita la integración con Pub/Sub y Cloud Functions permitiendo automatizar la carga de archivos fuente al bucket.
2. **Pub/Sub:**
Se eligió este servicio porque facilita la integración entre Cloud Storage y las distintas Cloud Functions que se necesiten en el proyecto. Este intermediario nos permite no depender de múltiples buckets para cada fuente de datos, sino por el contrario permite que múltiples cloud functions reaccionen a los mismos eventos de forma independiente y filtrada (por tipo de archivo y por carpeta dentro del bucket). Esto crea una arquitectura resiliente y escalable, donde podemos añadir, quitar o modificar procesadores de datos sin afectar el sistema de ingesta.
3. **Cloud Functions:**
Se eligió este servicio por ser una solución serverless (lo cual nos permite centrarnos en el código y pagando solo por el tiempo de ejecución), basada en eventos y de bajo costo. Facilita la ejecución de tareas de utilidad para este proyecto como:
   - Procesamiento de Eventos: Reaccionar a mensajes de Pub/Sub para procesar archivos.
   - Creación de APIs Simples: Exponer endpoints HTTP para simular fuentes de datos.
4. **BigQuery:**
Se eligió este servicio como el core de nuestra capa de almacenamiento y análisis debido a su capacidad para procesar un alto volumen de datos a alta velocidad, su motor SQL estándar y a su arquitectura serverless. Adicionalmente, sus funcionalidades de particionamiento por fecha y clustering por columnas clave se utilizaron para optimizar drásticamente el rendimiento de las consultas y minimizar los costos de escaneo, demostrando un diseño de almacenamiento eficiente y escalable para el futuro.
5. **IAM:**
Se implementó este servicio siguiendo el principio de menor privilegio. Para efectos de demostración, se habilitó la invocación pública en funciones HTTP; sin embargo, en un entorno de producción se utilizarían roles específicos, asegurando una comunicación segura y autenticada entre todos los componentes del pipeline.
6. **Looker Studio:**
Se eligió este servicio por su conexión nativa y sin costo con BigQuery. Su funcionalidad para ejecutar Custom Queries nos permitió generar dashboards interactivos y responder a las preguntas de negocio planteadas.

## Estrategia de Escalabilidad y Control de Costos en BigQuery

La solución implementada en BigQuery fue diseñada para garantizar la escalabilidad para manejar volúmenes de datos crecientes y mantener un control estricto sobre los costos de consultas SQL. Esto se logra a través de las siguientes consideraciones implementadas:

1. **Particionamiento de Tablas por Fecha:**
Las tablas de (ventas y eventos), que se espera que crezcan continuamente con el tiempo, fueron particionadas por su campo de fecha (fecha_venta y DATE(timestamp) respectivamente). Esta partición permite tener una optimización de costos importante. Al ejecutar consultas que incluyen un filtro de fecha "WHERE", BigQuery puede escanear únicamente las particiones relevantes en lugar de la tabla completa. A medida que las tablas crecen, el rendimiento de las consultas sobre períodos de tiempo específicos se mantiene alto, ya que el volumen de datos a escanear no aumenta si el rango de fechas de la consulta es constante.
2. **Clustering de Datos:**
El clustering ordena físicamente los datos dentro de cada partición. Cuando una consulta filtra por una columna clusterizada, BigQuery puede saltar directamente a los bloques de almacenamiento que contienen esos datos, evitando de nuevo un escaneo completo. Esto no solo acelera la consulta (mejor rendimiento), sino que también reduce los bytes procesados, contribuyendo al control de costos. Las tablas fueron clusterizadas por columnas que se usan frecuentemente en filtros, GROUPBY o JOINs.
   - ventas: Clusterizada por id_producto y id_cliente.
   - clientes: Clusterizada por id_cliente.
   - eventos: Clusterizada por id_cliente.

## Instrucciones de Ejecución

**Paso 1: Configuración del Entorno y Código Fuente**
- Clonar el Repositorio: Primero, obtener el código fuente clonando el repositorio Git.

   ```bash
   git clone https://github.com/[TU_USUARIO]/[TU_REPOSITORIO].git
   cd [NOMBRE_DEL_REPOSITORIO]

- Navegar al Directorio del Código Fuente: El código de todas las funciones se encuentra en un único directorio. Todos los comandos de despliegue (gcloud functions deploy) deben ejecutarse desde dentro de esta carpeta.

  ```bash
  # Asumiendo que la carpeta se llama 'mi-funcion'
  cd mi-funcion/

- Dentro de este directorio, se encuentran los dos archivos principales que albergan toda la lógica python:
    - main.py: Contiene el código Python para las cuatro Cloud Functions.
    - requirements.txt: Lista las dependencias de Python (google-cloud-bigquery, requests, google-cloud-storage).
 
- Establecer el Proyecto Activo y Habilitar APIs: Configurar terminal para que apunte al proyecto correcto y activar todos los servicios necesarios.
  ```bash
  gcloud config set project sapiens-assessment-pipiline

  gcloud services enable cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    pubsub.googleapis.com \
    bigquery.googleapis.com \
    storage-component.googleapis.com \
    iam.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com

**Paso 2: Crear la Infraestructura de Almacenamiento: Estos comandos deben ejecutarse desde la misma terminal, sin salir del directorio mi-funcion/**

- Crear el Bucket de Cloud Storage
  ```bash
  gsutil mb -p sapiens-assessment-pipiline -l US-CENTRAL1 gs://sapiens-pipiline-bucket

- Crear el Tópico de Pub/Sub
  ```bash
  gcloud pubsub topics create bucket-uploads-topic

- Crear las Tablas en BigQuery: Ejecutar los siguientes scripts SQL en la consola de BigQuery.
  
  Tabla de Ventas:
    ```sql
    CREATE OR REPLACE TABLE `sapiens-assessment-pipiline.sapiens_data.ventas` (
      id_venta STRING,
      fecha_venta DATE,
      id_cliente STRING,
      id_producto STRING,
      cantidad INT64,
      precio_unitario FLOAT64,
      descuento FLOAT64,
      canal_venta STRING
    )
    PARTITION BY fecha_venta
    CLUSTER BY id_producto, id_cliente;
    ```
  
  Tabla de Clientes:
    ```sql
    CREATE OR REPLACE TABLE `sapiens-assessment-pipiline.sapiens_data.clientes` (
      id_cliente STRING,
      nombre STRING,
      email STRING,
      fecha_registro DATE,
      segmento STRING,
      pais STRING
    )
    CLUSTER BY id_cliente;
    ```
  
  Tabla de Eventos:
    ```sql
    CREATE OR REPLACE TABLE `sapiens-assessment-pipiline.sapiens_data.eventos` (
      evento_id STRING,
      timestamp TIMESTAMP,
      id_cliente STRING,
      pagina STRING,
      accion STRING, 
      dispositivo STRING
    )
    PARTITION BY DATE(timestamp)
    CLUSTER BY id_cliente;
  ```

**Paso 3: Configurar los Flujos de Ingesta desde Cloud Storage**

- Crear Notificación para Ventas (CSV)
  ```bash
  gcloud storage buckets notifications create gs://sapiens-pipiline-bucket --topic=bucket-uploads-topic --event-types=OBJECT_FINALIZE --object-prefix=ventas/ --custom-attributes="file_type=csv"
  ```
- Crear Notificación para Eventos (JSON)
  ```bash
  gcloud storage buckets notifications create gs://sapiens-pipiline-bucket --topic=bucket-uploads-topic --event-types=OBJECT_FINALIZE --object-prefix=eventos/ --custom-attributes="file_type=json_eventos"
  ```

**Paso 4: Desplegar las Cloud Functions: Ejecutar estos comandos desde el directorio mi-funcion/**
- Desplegar Funciones Basadas en Eventos
  
  Función del Pipeline de Ventas:
    ```bash
    gcloud functions deploy procesador-ventas-csv --runtime=python311 --region=us-central1 --source=. --entry-point=procesa_csv_ventas --trigger-topic=bucket-uploads-topic --no-gen2
    ```
  Función del Pipeline de Eventos:
    ```bash
    gcloud functions deploy procesador-eventos-json --runtime=python311 --region=us-central1 --source=. --entry-point=procesa_json_eventos --trigger-topic=bucket-uploads-topic --no-gen2
    ```

- Desplegar Pipeline de Clientes (API)
  
  Desplegar la API simulada:
    ```bash
    gcloud functions deploy get-clientes-api --runtime=python311 --region=us-central1 --source=. --entry-point=get_clientes_api --trigger-http --no-gen2 --allow-unauthenticated
    ```
  Asignar permisos de acceso público:
    ```bash
    gcloud functions add-iam-policy-binding get-clientes-api --region=us-central1 --member=allUsers --role=roles/cloudfunctions.invoker
    ```
- Actualizar el código y desplegar la función que invoca al API:
  - La salida del comando anterior nos dará una URL. Debemos copiarla.
  - Abrir el archivo main.py en un editor de texto.
  - Buscar la función cargar_clientes_a_bigquery y reemplazar el placeholder de la variable API_URL con la URL que acabamos de copiar.
  - Guardar el archivo main.py.
  - Ahora, despliega la función que invoca al API:
      ```bash
      gcloud functions deploy cargar-clientes-a-bigquery --runtime=python311 --region=us-central1 --source=. --entry-point=cargar_clientes_a_bigquery --trigger-http --no-gen2 --allow-unauthenticated
      ```

**Paso 5: Ejecución y Prueba de los Pipelines**

- Pipeline de Ventas: Subir un archivo ventas.csv a la carpeta ventas/ en el bucket.
- Pipeline de Eventos: Subir un archivo JSON a la carpeta eventos/.
- Pipeline de Clientes: Llamar a la URL de la función cargar-clientes-a-bigquery desde un navegador.

Para cada paso, verificar los logs de las Cloud Functions y la tabla de destino en BigQuery.

**Paso 6: Ejecutar los siguientes scripts SQL para responder a las preguntas del Assessment:** 
- Teniendo como premisa que toda la data fue cargada correctamente en las tablas de Bigquery, se respetan los schemas y que el nombre del proyecto, datasets y tablas para estas consultas son las siguientes: 
  - Proyecto: sapiens-assessment-pipiline
  - Dataset: sapiens_data
  - Tablas: clientes, eventos, ventas

- Pregunta 1: Total de ventas por cliente últimos 6 meses
  ```sql
  SELECT
      c.id_cliente,
      c.nombre AS nombre_cliente,
      -- Calculamos el total de la venta considerando cantidad, precio y descuento.
      ROUND(SUM(v.cantidad * v.precio_unitario * (1 - v.descuento)),2) AS total_ventas_ultimos_6_meses
  FROM
      `sapiens-assessment-pipiline.sapiens_data.ventas` AS v
  JOIN
      `sapiens-assessment-pipiline.sapiens_data.clientes` AS c ON v.id_cliente = c.id_cliente
  WHERE
      -- Filtro de fecha que aprovecha la partición de la tabla de ventas.
      v.fecha_venta >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  GROUP BY
      c.id_cliente,
      c.nombre
  ORDER BY
      total_ventas_ultimos_6_meses DESC;
  ```   

  ![Pregunta1](https://github.com/user-attachments/assets/99104ea7-4270-4dbf-8601-512ec0151102)

- Pregunta 2: Clientes inactivos pero con navegación activa. Cabe resaltar que para esta pregunta alteramos uno de los datos del cliente C001 (fecha de navegación) ya que de no ser así el query no arroja coincidencias.

  ```sql
  
  WITH
  -- Paso 1: Obtener la fecha de la última compra de cada cliente
  ultima_compra_por_cliente AS (
      SELECT
          id_cliente,
          MAX(fecha_venta) AS fecha_ultima_compra
      FROM
          `sapiens-assessment-pipiline.sapiens_data.ventas`
      WHERE
          fecha_venta >= DATE '2025-01-01'
      GROUP BY
          id_cliente
  ),
  
  -- Paso 2: Obtener la fecha de la última navegación de cada cliente
  ultima_navegacion_por_cliente AS (
      SELECT
          id_cliente,
          MAX(DATE(timestamp)) AS fecha_ultima_navegacion
      FROM
          `sapiens-assessment-pipiline.sapiens_data.eventos`
      WHERE
          DATE(timestamp) >= DATE '2025-01-01'
      GROUP BY
          id_cliente
  )
  
  -- Paso 3: Unir y encontrar clientes cuya última navegación es más reciente que su última compra
  SELECT
      c.id_cliente,
      c.nombre,
      c.email,
      uc.fecha_ultima_compra,
      un.fecha_ultima_navegacion
  FROM
      `sapiens-assessment-pipiline.sapiens_data.clientes` AS c
  JOIN
      ultima_compra_por_cliente AS uc ON c.id_cliente = uc.id_cliente
  JOIN
      ultima_navegacion_por_cliente AS un ON c.id_cliente = un.id_cliente
  WHERE
      -- Condición clave: la última navegación ocurrió DESPUÉS de la última compra.
      un.fecha_ultima_navegacion > uc.fecha_ultima_compra
  ORDER BY
      c.id_cliente;
  ```
  ![Pregunta2](https://github.com/user-attachments/assets/dae440de-0739-451b-b249-fdb8e9e64a7e)

- Pregunta 3: Top 5 productos por margen de ganancia

  ```sql
  WITH
  -- Paso 1: Simular una tabla de costos de productos. Cabe resaltar que para esta consulta hemos simulado costos unitario para cada producto ya que sin esta información no podiamos completar el cálculo de margen de ganancia anual por producto
  costos_producto AS (
    SELECT 'P001' AS id_producto, 750.50 AS costo_unitario UNION ALL
    SELECT 'P002', 20.00 UNION ALL
    SELECT 'P003', 65.25 UNION ALL
    SELECT 'P004', 150.80 UNION ALL
    SELECT 'P005', 120.00 UNION ALL
    SELECT 'P006', 45.50 UNION ALL
    SELECT 'P007', 55.75 UNION ALL
    SELECT 'P008', 80.10 UNION ALL
    SELECT 'P009', 35.00 UNION ALL
    SELECT 'P010', 50.00 UNION ALL
    SELECT 'P011', 25.30 UNION ALL
    SELECT 'P012', 22.00 UNION ALL
    SELECT 'P013', 18.50 UNION ALL
    SELECT 'P014', 15.00 UNION ALL
    SELECT 'P015', 30.25 UNION ALL
    SELECT 'P016', 40.00 UNION ALL
    SELECT 'P017', 10.99 UNION ALL
    SELECT 'P018', 125.60 UNION ALL
    SELECT 'P019', 95.00 UNION ALL
    SELECT 'P020', 33.40
  )
  
  -- Paso 2: Unir las ventas con los costos simulados y calcular el margen
  SELECT
      v.id_producto,
      ROUND(SUM((v.precio_unitario * (1 - v.descuento) - c.costo_unitario) * v.cantidad),2) AS margen_total
  FROM
      `sapiens-assessment-pipiline.sapiens_data.ventas` AS v
  JOIN
      costos_producto AS c ON v.id_producto = c.id_producto
  WHERE
      -- Calculo del top 5 basado en las ventas del último año.
      v.fecha_venta >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY
      v.id_producto
  ORDER BY
      margen_total DESC
  LIMIT 5;
  ```

  ![Pregunta3](https://github.com/user-attachments/assets/eb6c847f-869d-4596-819e-93757d9cd166)

- Pregunta 4: Procedimiento almacenado para resumen mensual con partición. Para llamar al procedimiento podemos usar como ejemplo la siguiente sintaxis: "CALL `sapiens-assessment-pipiline.sapiens_data.sp_generar_resumen_mensual`('2025-06-01');"
 
  ```sql
  CREATE OR REPLACE PROCEDURE `sapiens-assessment-pipiline.sapiens_data.sp_generar_resumen_mensual`(mes_a_procesar DATE)
  BEGIN
    -- Este procedimiento calcula las métricas de venta para un mes dado
    -- y crea/reemplaza una tabla de resumen con los resultados.
    -- El mes de entrada debe ser cualquier día dentro del mes a procesar (ej. '2025-06-01').
  
    CREATE OR REPLACE TABLE `sapiens-assessment-pipiline.sapiens_data.resumen_mensual`
    PARTITION BY mes
    AS
    WITH
    -- Paso 1: Simular la tabla de costos de productos, igual que en la consulta 3
    costos_producto AS (
      SELECT 'P001' AS id_producto, 750.50 AS costo_unitario UNION ALL
      SELECT 'P002', 20.00 UNION ALL
      SELECT 'P003', 65.25 UNION ALL
      SELECT 'P004', 150.80 UNION ALL
      SELECT 'P005', 120.00 UNION ALL
      SELECT 'P006', 45.50 UNION ALL
      SELECT 'P007', 55.75 UNION ALL
      SELECT 'P008', 80.10 UNION ALL
      SELECT 'P009', 35.00 UNION ALL
      SELECT 'P010', 50.00 UNION ALL
      SELECT 'P011', 25.30 UNION ALL
      SELECT 'P012', 22.00 UNION ALL
      SELECT 'P013', 18.50 UNION ALL
      SELECT 'P014', 15.00 UNION ALL
      SELECT 'P015', 30.25 UNION ALL
      SELECT 'P016', 40.00 UNION ALL
      SELECT 'P017', 10.99 UNION ALL
      SELECT 'P018', 125.60 UNION ALL
      SELECT 'P019', 95.00 UNION ALL
      SELECT 'P020', 33.40
    )
    -- Paso 2: Generar el resumen uniendo ventas con los costos simulados
    SELECT
      DATE_TRUNC(v.fecha_venta, MONTH) AS mes,
      v.id_producto,
      -- Como no tenemos tabla de productos, no podemos obtener el nombre.
      -- Devolvemos el ID como identificador principal.
      SUM(v.cantidad) AS total_unidades_vendidas,
      ROUND(SUM(v.cantidad * v.precio_unitario * (1 - v.descuento)),2) AS ingresos_totales,
      ROUND(SUM((v.precio_unitario * (1 - v.descuento) - c.costo_unitario) * v.cantidad),2) AS margen_total
    FROM
      `sapiens-assessment-pipiline.sapiens_data.ventas` v
    JOIN
      costos_producto c ON v.id_producto = c.id_producto
    WHERE
      -- Filtramos para procesar solo el mes de interés, aprovechando la partición.
      -- Esto asegura que el procedimiento sea eficiente.
      DATE_TRUNC(v.fecha_venta, MONTH) = DATE_TRUNC(mes_a_procesar, MONTH)
    GROUP BY
      1, 2;
  
    -- Mensaje de confirmación que se mostrará al ejecutar el CALL
    SELECT FORMAT("Resumen para el mes %t generado con éxito en la tabla 'resumen_mensual'.", mes_a_procesar);
  
  END;
  ```
  
  ```sql
   -- Consulta 4: Visualizar data ordenada por margen total de la tabla resumen mensual
   SELECT
     *
   FROM
     `sapiens-assessment-pipiline.sapiens_data.resumen_mensual`
   ORDER BY
     margen_total DESC;
   ```
  ![Pregunta4](https://github.com/user-attachments/assets/d2b06772-b8c4-4015-87ea-60a11e081b4d)

  ## Dashboard en Looker Studio

  Se ha creado el siguiente dashboard en Looker Studio que busca presentar la data de cada consulta de una manera amigable y con utilidad para los casos de negocio que se puedan definir. Cada consulta hecha tiene un gráfico asociado diferenciado por su título. 

   ![Looker](https://github.com/user-attachments/assets/ae4a75ec-c995-4f41-bfb2-29c3b508dd05)

  ## Propuesta de mejoras del Pipeline

- **Seguridad Robusta entre Servicios:** Para facilitar la pronta ejecución del assessment, las Cloud Functions HTTP (get-clientes-api y cargar-clientes-a-bigquery) se configuraron para permitir invocaciones no autenticadas (--allow-unauthenticated). Esto representa un riesgo de seguridad significativo en un entorno real. Se propone asegurar la comunicación entre servicios utilizando correctamente las funcionalidades de IAM y gestionar así las credenciales de forma segura.
- **Concretar la simulación de "Stream de navegación web (eventos simulados desde Pub/Sub):** Debido a que posiblemente si indagabamos más sobre esta funcionalidad el tiempo nos hubiera podido jugar en contra, optamos por implementar esta fuente de datos similar a la fuente de ventas csv desde Cloud Storage, con la diferencia que en este caso el archivo fuente es .JSON. Se propone implementar la simulación a través de Pub/Sub para garantizar el manejo apropiado de distintas fuentes de datos.
- **Implementación de Pruebas de Calidad de Datos con Procedimientos:** Los pipelines actuales cargan los datos en BigQuery asumiendo que la óptima calidad de los datos de origen. No existe un proceso automatizado para validar que los datos cargados cumplen con las reglas de negocio esperadas. Por ejemplo, ¿qué pasaría si el archivo CSV de ventas llega con precios negativos o sin id_cliente en algunas filas? Estos "datos corruptos" se cargarían silenciosamente en la tabla, corrompiendo los resultados a las consultas SQL ya definidas. Se propone crear y programar procedimientos de validación en BigQuery que funcionen como pruebas de calidad de datos. Estos procedimientos se ejecutarían después de cada carga para verificar la integridad de los datos recién recibidos.

