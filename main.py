import base64
import json
import requests
from google.cloud import bigquery
from google.cloud import storage

client = bigquery.Client()

# ==========================================================
# FUNCIÓN 1: CARGADORA DE DATOS DE VENTAS A BIGQUERY
# ==========================================================

def procesa_csv_ventas(cloud_event, context):
    # Función activada por Pub/Sub que procesa un CSV de Cloud Storage y carga los datos en la tabla de BigQuery usando su schema existente
    # Filtro de atributos para asegurarse de que es el mensaje correcto
    attributes = cloud_event.get('attributes', {})
    if attributes.get('file_type') != 'csv':
        print(f"Función ventas ignoró mensaje. Atributos: {attributes}")
        return

    # Accedemos directamente al campo 'data' del evento, que contiene el mensaje codificado.
    if 'data' in cloud_event:
        pubsub_message = base64.b64decode(cloud_event['data']).decode('utf-8')
        event_data = json.loads(pubsub_message)
    else:
        print("ERROR: El evento no contiene el campo 'data'.")
        return # Termina si el evento no tiene el formato esperado

    # Obtenemos el nombre del bucket y file del archivo cargado que gatilló el trigger para poder leerlo
    bucket_name = event_data['bucket']
    file_name = event_data['name']

    print(f"Función activada para procesar archivo CSV: {file_name}")

    # Dado que la tabla ya fue creada previamente, conocemos los datos y no es necesario definir un schema
    uri = f"gs://{bucket_name}/{file_name}"
    table_id = "sapiens-assessment-pipiline.sapiens_data.ventas"
    
    # Configurar el trabajo de carga a BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )
    
    # Carga de la data a la tabla ventas
    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        print(f"Éxito: Se cargó el archivo {file_name} a la tabla {table_id}")
    except Exception as e:
        print(f"ERROR al cargar a BigQuery: {e}")
        raise e
    
# =================================================================
# FUNCIÓN 2: CARGADORA DE DATOS DE EVENTOS A BIGQUERY
# =================================================================
def procesa_json_eventos(cloud_event, context):
    # Función activada por Pub/Sub que procesa un JSON de Cloud Storage y carga los datos en la tabla de BigQuery usando su schema existente
    # Filtro de atributos para asegurarse de que es el mensaje correcto
    attributes = cloud_event.get('attributes', {})
    if attributes.get('file_type') != 'json_eventos':
        print(f"Función Eventos ignoró mensaje. Atributos: {attributes}")
        return
    # Accedemos directamente al campo 'data' del evento, que contiene el mensaje codificado.
    if 'data' in cloud_event:
        pubsub_message = base64.b64decode(cloud_event['data']).decode('utf-8')
        event_data = json.loads(pubsub_message)
    else:
        print("ERROR: El evento no contiene el campo 'data'.")
        return # Termina si el evento no tiene el formato esperado
    
    # Obtenemos el nombre del bucket y file del archivo cargado que gatilló el trigger para poder leerlo
    bucket_name = event_data['bucket']
    file_name = event_data['name']
    
    print(f"Función activada para procesar archivo JSON: {file_name}")

    # Dado que la tabla ya fue creada previamente, conocemos los datos y no es necesario definir un schema
    table_id = "sapiens-assessment-pipiline.sapiens_data.eventos"
    
    try:
        # Leer el archivo directamente desde Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Descargar el contenido del archivo como una cadena de texto
        json_data_str = blob.download_as_text()

        # Parsear la cadena de texto a una lista de diccionarios Python
        json_data = json.loads(json_data_str)
        
        if not json_data:
            print("El archivo JSON está vacío. No hay nada que cargar.")
            return

        print(f"Se leyeron {len(json_data)} eventos del archivo JSON.")

        # Configurar el trabajo de carga a BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        # Carga de los datos desde la memoria (la lista de diccionarios) a la tabla eventos
        load_job = client.load_table_from_json(json_data, table_id, job_config=job_config)
        load_job.result()
        print(f"Éxito: Se cargó el archivo {file_name} a la tabla {table_id}")

    except Exception as e:
        print(f"ERROR al cargar a BigQuery: {e}")
        raise e

# ==========================================================
# FUNCIÓN INTERMEDIA: SIMULACIÓN CONSULTA API CLIENTES
# ==========================================================

def get_clientes_api(request):
    # Una Cloud Function con trigger HTTP que simula una API de clientes. Devuelve una lista de clientes en formato JSON.
    datos_clientes = [
        {"id_cliente": "C001", "nombre": "Cliente 1", "email": "cliente1@correo.com", "fecha_registro": "2024-01-07", "segmento": "Básico", "pais": "Chile"},
        {"id_cliente": "C002", "nombre": "Cliente 2", "email": "cliente2@correo.com", "fecha_registro": "2024-03-12", "segmento": "Premium", "pais": "Chile"},
        {"id_cliente": "C003", "nombre": "Cliente 3", "email": "cliente3@correo.com", "fecha_registro": "2024-03-04", "segmento": "Premium", "pais": "Colombia"},
        {"id_cliente": "C004", "nombre": "Cliente 4", "email": "cliente4@correo.com", "fecha_registro": "2024-04-13", "segmento": "Estándar", "pais": "Perú"},
        {"id_cliente": "C005", "nombre": "Cliente 5", "email": "cliente5@correo.com", "fecha_registro": "2024-06-17", "segmento": "Premium", "pais": "Colombia"},
        {"id_cliente": "C006", "nombre": "Cliente 6", "email": "cliente6@correo.com", "fecha_registro": "2024-04-08", "segmento": "Premium", "pais": "Perú"},
        {"id_cliente": "C007", "nombre": "Cliente 7", "email": "cliente7@correo.com", "fecha_registro": "2024-01-06", "segmento": "Básico", "pais": "Perú"},
        {"id_cliente": "C008", "nombre": "Cliente 8", "email": "cliente8@correo.com", "fecha_registro": "2024-01-01", "segmento": "Estándar", "pais": "Colombia"},
        {"id_cliente": "C009", "nombre": "Cliente 9", "email": "cliente9@correo.com", "fecha_registro": "2024-01-28", "segmento": "Básico", "pais": "Colombia"},
        {"id_cliente": "C010", "nombre": "Cliente 10", "email": "cliente10@correo.com", "fecha_registro": "2024-01-14", "segmento": "Estándar", "pais": "Colombia"}
    ]

    return (datos_clientes, 200)

# ==========================================================
# FUNCIÓN 3: CARGADORA DE DATOS DE CLIENTES A BIGQUERY
# ==========================================================
def cargar_clientes_a_bigquery(request):
    
    # Función HTTP que llama a la API de clientes, obtiene los datos y los carga en la tabla de BigQuery.
    # URL de la API que acabas de desplegar. Obtenida de la función intermedia get-clientes-api
    API_URL = "https://us-central1-sapiens-assessment-pipiline.cloudfunctions.net/get-clientes-api"
    
    TABLE_ID = "sapiens-assessment-pipiline.sapiens_data.clientes"
    client = bigquery.Client()

    try:
        # Llamar a la API
        print("Llamando a la API de clientes...")
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status() 
        
        # La API ya devuelve un JSON listo para cargar
        clientes_data = response.json()
        print(f"Se recibieron {len(clientes_data)} registros de clientes.")

        # Configurar el trabajo de carga a BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        # Carga de los datos desde la memoria a la tabla clientes
        job = client.load_table_from_json(clientes_data, TABLE_ID, job_config=job_config)
        job.result()

        print(f"Éxito: Se cargaron {len(clientes_data)} registros en la tabla {TABLE_ID}.")
        return ("Carga de clientes completada con éxito.", 200)

    except requests.exceptions.RequestException as e:
        print(f"Error al llamar a la API: {e}")
        return (f"Error al llamar a la API: {e}", 500)
    except Exception as e:
        print(f"Error al cargar a BigQuery: {e}")
        return (f"Error al cargar a BigQuery: {e}", 500)

