# Parte 3: Liderazgo y Observabilidad

## 1. Configuración de SLA en Airflow

Para monitorear un proceso crítico de carga de Power BI que debe finalizar antes de las 6:00 AM, utilizamos la funcionalidad nativa de **SLA (Service Level Agreement)** de Airflow.

### Pasos de Configuración:
1.  **Definir SLA en `default_args`**:
    ```python
    default_args = {
        'owner': 'equipo_datos',
        'start_date': datetime(2023, 1, 1),
        'sla': timedelta(hours=4), # Si el DAG comienza a las 2:00 AM, debe terminar a las 6:00 AM
    }
    ```
2.  **SLA Miss Callback**: Implementar una función para manejar los incumplimientos (ej. alertas de Slack, PagerDuty).
    ```python
    def my_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_configs):
        # Notificar a los interesados sin interrumpir el proceso
        send_alert(f"CRÍTICO: El DAG {dag.dag_id} incumplió el SLA. Tareas retrasadas: {task_list}")
    ```
3.  **Registro**: Asignar el callback al DAG.
    ```python
    with DAG('pb_load_dag', default_args=default_args, sla_miss_callback=my_sla_miss_callback) as dag:
        ...
    ```

---

## 2. Guía Técnica: Revisión de Código para un Data Engineer Junior

**Contexto**: Revisión por pares de código de un DE Junior que utiliza `SELECT *` y carga datasets completos en la memoria de Airflow.

### Checklist de Retroalimentación:

#### 1. Rendimiento: Evitar `SELECT *`
-   **Por qué**: `SELECT *` recupera columnas innecesarias, aumentando el I/O de red y las lecturas de disco. Además, es frágil ante cambios en el esquema de la tabla subyacente.
-   **Corrección**: Enumerar explícitamente solo las columnas requeridas para la transformación o la tabla destino.
-   **Tip de Código**: `SELECT device_id, metric_value FROM table_telemetry;`

#### 2. Gestión de Recursos: Evitar el exceso de memoria en Airflow
-   **Por qué**: Airflow es un **orquestador**, no un motor de procesamiento. Cargar millones de filas en un DataFrame de Pandas dentro de un Worker provocará errores de `Out Of Memory (OOM)` e inestabilidad en el cluster.
-   **Corrección**: 
    -   **Push-down**: Usar SQL para realizar transformaciones dentro de Postgres (ELT).
    -   **Streaming/Chunking**: Si una transformación en Python es obligatoria, usar `chunksize` en Pandas o transmitir los datos.
    -   **Offload**: Para volúmenes altos, usar motores de cómputo como Spark, Snowflake o BigQuery.

#### 3. Mejores Prácticas (General)
-   **Idempotencia**: "¿Este código se ejecuta de forma segura si lo ejecuto dos veces para el mismo día?"
-   **Logs**: "¿Estamos registrando la cantidad de filas procesadas para poder depurar rápidamente?"
-   **Tipado**: "¿Estás usando Type Hints para que el código sea autodocumentado?"

---

## 3. Implementación de Observabilidad (Logs del Sistema)
El pipeline implementado (Parte 2) registra:
1.  `Tiempo de Inicio/Fin`: Rastreado por Airflow naturalmente y calculado en el `PostgresUpsertOperator`.
2.  `Filas Procesadas`: Contabilizadas después de la extracción y durante la carga.
3.  `Filas Rechazadas (DQ)`: Si la lógica del umbral de nulos falla, el mensaje de error registra el conteo exacto de registros inválidos.
