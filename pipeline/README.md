# Parte 2: Implementación Técnica - Pipeline de Datos de Telemetría

## Descripción General
Este pipeline implementa un proceso de ingesta incremental desde archivos CSV (simulando S3/Blob storage) hacia un Data Warehouse en Postgres.

## Características Clave
1.  **Idempotencia**: Utiliza un `PostgresUpsertOperator` personalizado que realiza un `INSERT ON CONFLICT (id) DO UPDATE` para permitir re-ejecuciones seguras (Backfills) sin duplicar registros.
2.  **Calidad de Datos (DQ)**: 
    - Valida columnas críticas (`id`, `device_id`, `timestamp`).
    - Falla el pipeline explícitamente si el ratio de registros nulos excede el **5%**.
3.  **Observabilidad**:
    - Registra tiempos de inicio/fin, filas totales procesadas y métricas de DQ.
    - Utiliza `XComs` de Airflow para compartir métricas entre tareas (Metadatos).
4.  **Evolución de Esquema**: El operador personalizado construye dinámicamente la sentencia UPSERT basada en las columnas del DataFrame de entrada.

## Configuración Local (Simulación)
1.  **Generar Datos**:
    ```bash
    python pipeline/scripts/generate_data.py
    ```
    Esto crea `pipeline/data/telemetry_logs.csv` con registros simulados y algunos nulos.

2.  **Despliegue en Airflow**:
    - Agregue `pipeline/plugins` a su carpeta de `plugins` de Airflow.
    - Agregue `pipeline/dags/telemetry_ingestion_dag.py` a su carpeta de `dags`.
    - Asegúrese de que la conexión de Postgres `postgres_dwh` esté configurada.

## Implementación de Observabilidad
El pipeline registra métricas en los logs de la instancia de tarea de Airflow:
- `Métricas DQ`: Registradas durante la tarea `extract_and_validate_dq`.
- `Duración`: Calculada y registrada en el resumen de ejecución de la tarea `upsert_to_silver`.
