from datetime import datetime, timedelta
from airflow import DAG  # type: ignore[import]
from airflow.operators.python import PythonOperator  # type: ignore[import]
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import]

# En Airflow, la carpeta 'plugins' se añade al PYTHONPATH automáticamente.
# Para que este import funcione localmente, añade 'pipeline/plugins' a tu PYTHONPATH.
from operators.postgres_upsert_operator import PostgresUpsertOperator  # type: ignore[import]

import pandas as pd  # type: ignore[import]
import logging
import os

# =============================================================================
# CONFIGURACIÓN GLOBAL
# =============================================================================
CSV_SOURCE      = os.environ.get('TELEMETRY_CSV_PATH', 'pipeline/data/telemetry_logs.csv')
POSTGRES_CONN_ID = 'postgres_dwh'
SCHEMA_CORE     = 'silver'
TABLE_NAME      = 'fact_telemetry'
NULL_THRESHOLD  = 0.05   # 5% — Umbral máximo de registros nulos en columnas críticas
CRITICAL_COLS   = ['id', 'device_id', 'timestamp']


# =============================================================================
# CALLBACK DE SLA
# CRITERIO: Observabilidad — Alertas cuando el pipeline no termina a las 6:00 AM
# =============================================================================
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_configs):
    """
    Se ejecuta automáticamente por Airflow cuando alguna tarea supera su SLA.

    En producción esto enviaría una notificación a Slack, PagerDuty o correo,
    sin interrumpir la ejecución del pipeline (solo registra el incumplimiento).
    """
    logging.critical(
        f"[SLA MISS] El DAG '{dag.dag_id}' ha incumplido el SLA. "
        f"Tareas retrasadas: {task_list}. "
        f"El proceso de carga a Power BI podría no estar listo para las 6:00 AM."
    )
    # Ejemplo de integración con sistema de alertas externo:
    # send_slack_alert(channel="#alertas-datos", message=...)
    # send_pagerduty_alert(severity="critical", summary=...)


# =============================================================================
# ARGUMENTOS POR DEFECTO
# =============================================================================
default_args = {
    'owner': 'senior_de',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # SLA de 4 horas: si el DAG arranca a las 2:00 AM, debe finalizar a las 6:00 AM
    'sla': timedelta(hours=4),
}


# =============================================================================
# TAREA 1: EXTRACCIÓN Y VALIDACIÓN DE CALIDAD DE DATOS
# CRITERIOS CUBIERTOS:
#   - Observabilidad: tiempo inicio/fin, filas totales, procesadas y rechazadas
#   - Validación DQ: error explícito si >5% de nulos en columnas críticas
# =============================================================================
def extract_and_validate_dq(**kwargs):
    """
    Extrae datos del CSV de origen, aplica chequeos de Calidad de Datos (DQ)
    y publica los resultados en XCom para ser consumidos por las tareas siguientes.

    Flujo:
    1. Registrar tiempo de inicio.
    2. Leer el CSV de origen.
    3. Identificar y contar filas con nulos en columnas críticas (RECHAZADAS).
    4. Loggear métricas completas: total, válidas, rechazadas.
    5. Fallar explícitamente si el ratio de rechazo supera el 5%.
    6. Publicar datos limpios y métricas en XCom.
    7. Registrar tiempo de fin.

    Returns:
        int: Número de filas válidas que pasan a la carga.

    Raises:
        FileNotFoundError: Si el archivo CSV de origen no existe.
        ValueError: Si el ratio de nulos supera el umbral del 5%.
    """
    start_time = datetime.now()
    logical_date = kwargs.get('logical_date') or kwargs.get('ds')

    logging.info("=" * 60)
    logging.info("[INICIO] Tarea: extract_and_validate_dq")
    logging.info(f"[INICIO] Fecha lógica (ds): {logical_date}")
    logging.info(f"[INICIO] Timestamp: {start_time.isoformat()}")
    logging.info("=" * 60)

    # -------------------------------------------------------------------------
    # 1. Verificar existencia del archivo de origen
    # -------------------------------------------------------------------------
    if not os.path.exists(CSV_SOURCE):
        raise FileNotFoundError(
            f"[ERROR] Archivo de origen no encontrado: '{CSV_SOURCE}'. "
            "Ejecuta generate_data.py para crear los datos de prueba."
        )

    # -------------------------------------------------------------------------
    # 2. Extracción: Leer CSV con soporte de Evolución de Esquema
    # El CSV puede contener columnas nuevas; Pandas las incorpora automáticamente.
    # La detección en la tabla Postgres se delega al PostgresUpsertOperator.
    # -------------------------------------------------------------------------
    logging.info(f"[EXTRACCIÓN] Leyendo archivo: {CSV_SOURCE}")
    df = pd.read_csv(CSV_SOURCE)
    total_rows = len(df)
    source_columns = df.columns.tolist()

    logging.info(f"[EXTRACCIÓN] Filas totales leídas : {total_rows}")
    logging.info(f"[EXTRACCIÓN] Columnas detectadas  : {source_columns}")

    # -------------------------------------------------------------------------
    # 3. Validación de Calidad de Datos (DQ)
    # CRITERIO: Detectar nulos en clave primaria ('id') y campos de fecha críticos
    # -------------------------------------------------------------------------
    logging.info(f"[DQ] Validando columnas críticas: {CRITICAL_COLS}")

    # Máscara de filas que tienen AL MENOS UN nulo en columnas críticas
    null_mask = df[CRITICAL_COLS].isnull().any(axis=1)
    rows_rejected = int(null_mask.sum())
    rows_valid    = total_rows - rows_rejected
    null_ratio    = rows_rejected / total_rows if total_rows > 0 else 0.0

    # -------------------------------------------------------------------------
    # 4. Logging de observabilidad — FILAS RECHAZADAS
    # CRITERIO: Registrar cantidad de filas rechazadas por calidad ANTES de fallar
    # -------------------------------------------------------------------------
    logging.info("[OBSERVABILIDAD - DQ] ─────────────────────────────")
    logging.info(f"[OBSERVABILIDAD - DQ] Filas totales    : {total_rows}")
    logging.info(f"[OBSERVABILIDAD - DQ] Filas válidas    : {rows_valid}")
    logging.info(f"[OBSERVABILIDAD - DQ] Filas rechazadas : {rows_rejected}")
    logging.info(f"[OBSERVABILIDAD - DQ] Ratio de rechazo : {null_ratio:.2%}")
    logging.info(f"[OBSERVABILIDAD - DQ] Umbral permitido : {NULL_THRESHOLD:.2%}")
    logging.info("[OBSERVABILIDAD - DQ] ─────────────────────────────")

    # -------------------------------------------------------------------------
    # 5. Publicar métricas DQ en XCom para trazabilidad downstream
    # El PostgresUpsertOperator las consumirá para incluirlas en su resumen.
    # -------------------------------------------------------------------------
    dq_metrics = {
        'total_rows'    : total_rows,
        'rows_valid'    : rows_valid,
        'rows_rejected' : rows_rejected,
        'null_ratio'    : float(f"{null_ratio:.6f}"),
        'source_columns': source_columns,
        'critical_cols' : CRITICAL_COLS,
        'logical_date'  : str(logical_date),
        'start_time'    : start_time.isoformat(),
    }
    kwargs['ti'].xcom_push(key='dq_metrics', value=dq_metrics)

    # -------------------------------------------------------------------------
    # 6. CRITERIO: Fallar explícitamente si el ratio supera el umbral del 5%
    # -------------------------------------------------------------------------
    if null_ratio > NULL_THRESHOLD:
        logging.error(
            f"[DQ FALLO] El ratio de rechazo {null_ratio:.2%} supera el umbral "
            f"permitido del {NULL_THRESHOLD:.2%}. "
            f"Se rechazaron {rows_rejected} de {total_rows} registros. "
            "Pipeline abortado para proteger la integridad de la capa Silver."
        )
        raise ValueError(
            f"Validación de Calidad de Datos FALLIDA: "
            f"{rows_rejected}/{total_rows} registros ({null_ratio:.2%}) "
            f"tienen nulos en columnas críticas {CRITICAL_COLS}."
        )

    logging.info(f"[DQ OK] Calidad de datos aceptable. Procediendo con {rows_valid} filas válidas.")

    # -------------------------------------------------------------------------
    # 7. Filtrar y publicar datos limpios en XCom
    # Solo se pasan a la capa Silver las filas que superaron los chequeos DQ.
    # -------------------------------------------------------------------------
    df_clean = df[~null_mask]
    kwargs['ti'].xcom_push(key='clean_data', value=df_clean.to_json())

    # -------------------------------------------------------------------------
    # 8. Registrar tiempo de fin y duración total
    # -------------------------------------------------------------------------
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logging.info("=" * 60)
    logging.info("[FIN] Tarea: extract_and_validate_dq")
    logging.info(f"[FIN] Timestamp: {end_time.isoformat()}")
    logging.info(f"[OBSERVABILIDAD] Tiempo inicio     : {start_time.isoformat()}")
    logging.info(f"[OBSERVABILIDAD] Tiempo fin        : {end_time.isoformat()}")
    logging.info(f"[OBSERVABILIDAD] Duración tarea    : {duration:.2f}s")
    logging.info(f"[OBSERVABILIDAD] Filas procesadas  : {rows_valid}")
    logging.info(f"[OBSERVABILIDAD] Filas rechazadas  : {rows_rejected}")
    logging.info("=" * 60)

    return rows_valid


# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
with DAG(
    dag_id='telemetry_ingestion_v2',
    default_args=default_args,
    description=(
        'Pipeline de ingesta incremental de logs de telemetría. '
        'Implementa Arquitectura Medallion (Bronze→Silver→Gold), '
        'UPSERT idempotente, evolución dinámica de esquema, '
        'validación DQ con umbral del 5% y observabilidad completa.'
    ),
    schedule_interval='0 2 * * *',  # Diariamente a las 2:00 AM
    catchup=False,                  # No re-ejecutar fechas pasadas automáticamente
    sla_miss_callback=sla_miss_callback,
    tags=['telemetry', 'medallion', 'idempotent', 'silver'],
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # TAREA 1: Extracción + Validación de Calidad de Datos
    # ─────────────────────────────────────────────────────────────────────────
    extract_task = PythonOperator(
        task_id='extract_and_validate_dq',
        python_callable=extract_and_validate_dq,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TAREA 2: Carga Idempotente a Silver (UPSERT + Evolución de Esquema)
    #
    # El PostgresUpsertOperator se encarga de:
    # - Detectar columnas nuevas y hacer ALTER TABLE automáticamente.
    # - Ejecutar INSERT ON CONFLICT para garantizar idempotencia.
    # - Registrar métricas completas de carga.
    # ─────────────────────────────────────────────────────────────────────────
    load_to_silver = PostgresUpsertOperator(
        task_id='upsert_to_silver',
        postgres_conn_id=POSTGRES_CONN_ID,
        table_name=f"{SCHEMA_CORE}.{TABLE_NAME}",
        unique_key='id',
        # Los datos y métricas se consumen desde XCom dentro del operator
        dq_metrics_key='dq_metrics',
    )

    # ─────────────────────────────────────────────────────────────────────────
    # DEPENDENCIAS: Extract → Load
    # ─────────────────────────────────────────────────────────────────────────
    extract_task >> load_to_silver
