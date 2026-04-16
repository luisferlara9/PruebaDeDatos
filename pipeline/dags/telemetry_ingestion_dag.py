from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# En Airflow, la carpeta 'plugins' se añade al PYTHONPATH automáticamente.
# Para que este import funcione localmente, asegúrate de añadir 'pipeline/plugins' a tu PYTHONPATH.
from operators.postgres_upsert_operator import PostgresUpsertOperator
import pandas as pd
import logging
import os

# Configuración
CSV_SOURCE = os.environ.get('TELEMETRY_CSV_PATH', 'pipeline/data/telemetry_logs.csv')
POSTGRES_CONN_ID = 'postgres_dwh'
SCHEMA_CORE = 'silver'
TABLE_NAME = 'fact_telemetry'
NULL_THRESHOLD = 0.05

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_configs):
    """
    Callback personalizado para incumplimientos de SLA.
    """
    logging.warning(f"SLA Incumplido para el DAG {dag.dag_id}. Tareas: {task_list}")
    # Aquí normalmente enviarías una alerta a Slack, PagerDuty o Correo
    # Ejemplo: send_slack_notification(f"El proceso no terminó a las 6:00 AM")

default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=6), # Ejemplo: Se espera que el proceso termine en 6 horas desde la logical_date
}

def extract_and_validate_dq(**kwargs):
    """
    Extrae datos y realiza chequeos de Calidad de Datos (DQ).
    Falla si > 5% de los registros tienen nulos en columnas críticas.
    """
    logical_date = kwargs.get('logical_date')
    logging.info(f"Iniciando extracción para la fecha lógica: {logical_date}")
    
    if not os.path.exists(CSV_SOURCE):
        raise FileNotFoundError(f"Archivo de origen {CSV_SOURCE} no encontrado.")
        
    df = pd.read_csv(CSV_SOURCE)
    total_rows = len(df)
    
    # Chequeo DQ: Nulos en clave primaria o campos críticos
    critical_cols = ['id', 'device_id', 'timestamp']
    null_counts = df[critical_cols].isnull().sum().sum()
    null_ratio = null_counts / total_rows
    
    logging.info(f"Métricas DQ - Filas Totales: {total_rows}, Registros Nulos en Columnas Críticas: {null_counts}")
    
    if null_ratio > NULL_THRESHOLD:
        logging.error(f"FALLO DE DQ: El ratio de nulos {null_ratio:.2%} excede el umbral {NULL_THRESHOLD:.2%}")
        raise ValueError("El chequeo de Calidad de Datos falló: Demasiados nulos en columnas críticas.")
    
    # Almacenar datos limpios en XCom (Nota: para datasets grandes, usar rutas de S3/GCS)
    kwargs['ti'].xcom_push(key='clean_data', value=df.to_json())
    return total_rows

with DAG(
    'telemetry_ingestion_v1',
    default_args=default_args,
    description='Ingesta incremental de logs de telemetría con UPSERT y chequeos de DQ',
    schedule_interval='0 2 * * *', # Diariamente a las 2:00 AM
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=['telemetry', 'idempotent', 'gold'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_validate_dq',
        python_callable=extract_and_validate_dq,
        provide_context=True,
    )

    # Nota: En un escenario real con 50M de registros, no pasaríamos el DF por XCom.
    # Usaríamos una tabla de staging en Postgres o un bucket de almacenamiento.
    
    load_to_silver = PostgresUpsertOperator(
        task_id='upsert_to_silver',
        postgres_conn_id=POSTGRES_CONN_ID,
        table_name=f"{SCHEMA_CORE}.{TABLE_NAME}",
        unique_key='id',
        df="{{ ti.xcom_pull(task_ids='extract_and_validate_dq', key='clean_data') }}",
    )

    extract_task >> load_to_silver
