from __future__ import annotations

from typing import List, Optional
from datetime import datetime
import logging

# Los módulos de Airflow y Pandas son dependencias de runtime disponibles en el
# entorno de Airflow. El linter local (Pyre2) no los encuentra porque no están
# instalados en el virtualenv de desarrollo — esto es esperado y aceptable.
from airflow.models import BaseOperator  # type: ignore[import]
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import]
from airflow.utils.decorators import apply_defaults  # type: ignore[import]
import pandas as pd  # type: ignore[import]


class PostgresUpsertOperator(BaseOperator):
    """
    Operador personalizado para realizar operaciones UPSERT (INSERT ON CONFLICT) en Postgres.

    Responsabilidades:
    1. Evolución Dinámica de Esquema: Detecta columnas nuevas en el origen y las
       agrega automáticamente a la tabla destino vía ALTER TABLE, sin romper el pipeline.
    2. Idempotencia: Usa ON CONFLICT (unique_key) DO UPDATE para garantizar que
       re-ejecuciones para la misma fecha lógica no dupliquen registros.
    3. Observabilidad: Registra tiempo de inicio/fin, filas procesadas y filas rechazadas.
    """

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        postgres_conn_id: str = 'postgres_default',
        unique_key: str = 'id',
        df_json: Optional[str] = None,
        dq_metrics_key: str = 'dq_metrics',
        *args,
        **kwargs
    ):
        super(PostgresUpsertOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.unique_key = unique_key
        self.df_json = df_json
        self.dq_metrics_key = dq_metrics_key

    def _parse_table_parts(self):
        """Separa el nombre de la tabla en esquema y nombre de tabla."""
        parts = self.table_name.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        return 'public', parts[0]

    def _ensure_schema_evolution(self, hook: PostgresHook, df: pd.DataFrame) -> list:
        """
        CRITERIO: Manejo de Esquemas Dinámicos.

        Compara las columnas del DataFrame de origen con las columnas existentes
        en la tabla destino de Postgres. Si encuentra columnas nuevas, las agrega
        automáticamente usando ALTER TABLE ... ADD COLUMN IF NOT EXISTS.

        Esto permite que la adición de nuevas columnas en los archivos de origen
        no rompa el pipeline (evolución de esquema sin intervención manual).

        Args:
            hook: Conexión a Postgres.
            df: DataFrame con los datos del origen.

        Returns:
            Lista de columnas nuevas que fueron agregadas.
        """
        schema, table = self._parse_table_parts()

        # Consultar columnas existentes en la tabla destino
        existing_cols_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name   = '{table}';
        """
        try:
            existing_cols_rows = hook.get_records(existing_cols_query)
            existing_cols = {row[0].lower() for row in existing_cols_rows}
        except Exception:
            # Si la tabla aún no existe no hacemos nada; la creación es responsabilidad de las migraciones.
            logging.warning(
                f"No se pudo consultar el esquema de {self.table_name}. "
                "Se asume que la tabla todavía no existe o no es accesible."
            )
            return []

        # Identificar columnas nuevas: existen en el origen pero no en el destino
        source_cols = [col.lower() for col in df.columns]
        new_cols = [col for col in source_cols if col not in existing_cols]

        for col in new_cols:
            # Usamos TEXT como tipo seguro por defecto. En producción esto podría
            # inferirse del dtype del DataFrame o de un catálogo de tipos.
            alter_sql = f"""
                ALTER TABLE {self.table_name}
                ADD COLUMN IF NOT EXISTS {col} TEXT;
            """
            hook.run(alter_sql)
            logging.info(
                f"[SCHEMA EVOLUTION] Columna nueva detectada y agregada: "
                f"'{col}' → {self.table_name} (tipo TEXT)"
            )

        if not new_cols:
            logging.info("[SCHEMA EVOLUTION] Sin cambios de esquema detectados.")

        return new_cols

    def execute(self, context):
        """
        Ejecuta el UPSERT en Postgres con observabilidad completa.

        Flujo:
        1. Recuperar datos limpios desde XCom.
        2. Aplicar evolución de esquema dinámica.
        3. Construir y ejecutar la sentencia UPSERT.
        4. Registrar métricas de observabilidad (inicio, fin, filas, rechazadas).
        """
        start_time = datetime.now()
        logging.info("=" * 60)
        logging.info(f"[INICIO] PostgresUpsertOperator → {self.table_name}")
        logging.info(f"[INICIO] Timestamp: {start_time.isoformat()}")
        logging.info("=" * 60)

        # --- Recuperar datos desde XCom ---
        ti = context['ti']
        raw_json = ti.xcom_pull(
            task_ids='extract_and_validate_dq',
            key='clean_data'
        )

        # Recuperar métricas DQ publicadas por la tarea anterior
        dq_metrics = ti.xcom_pull(
            task_ids='extract_and_validate_dq',
            key=self.dq_metrics_key
        ) or {}
        rows_rejected = dq_metrics.get('rows_rejected', 'N/A')

        if raw_json is None:
            logging.warning("[CARGA] No hay datos para cargar. Abortando tarea.")
            return 0

        df = pd.read_json(raw_json)

        if df.empty:
            logging.warning("[CARGA] El DataFrame está vacío. No se realizará ninguna operación.")
            return 0

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # --- 1. Evolución Dinámica de Esquema ---
        new_cols = self._ensure_schema_evolution(hook, df)

        # --- 2. Construir consulta UPSERT ---
        cols = ", ".join(df.columns.tolist())
        placeholders = ", ".join(["%s"] * len(df.columns))
        update_parts = [
            f"{col} = EXCLUDED.{col}"
            for col in df.columns
            if col != self.unique_key
        ]
        update_stmt = ", ".join(update_parts)

        sql = f"""
            INSERT INTO {self.table_name} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({self.unique_key})
            DO UPDATE SET {update_stmt};
        """

        logging.info(f"[SQL] Sentencia UPSERT generada para tabla: {self.table_name}")
        logging.info(f"[SQL] Columnas: {cols}")
        logging.info(f"[SQL] Clave de conflicto: {self.unique_key}")

        # --- 3. Ejecutar UPSERT por lotes ---
        values = [tuple(row) for row in df.to_numpy()]

        hook.run(
            sql,
            parameters=values,
            autocommit=True
        )

        # --- 4. Registrar métricas de observabilidad ---
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        rows_processed = len(df)

        logging.info("=" * 60)
        logging.info(f"[FIN] PostgresUpsertOperator → {self.table_name}")
        logging.info(f"[FIN] Timestamp: {end_time.isoformat()}")
        logging.info(f"[OBSERVABILIDAD] Tiempo de inicio   : {start_time.isoformat()}")
        logging.info(f"[OBSERVABILIDAD] Tiempo de fin      : {end_time.isoformat()}")
        logging.info(f"[OBSERVABILIDAD] Duración total     : {duration_seconds:.2f}s")
        logging.info(f"[OBSERVABILIDAD] Filas procesadas   : {rows_processed}")
        logging.info(f"[OBSERVABILIDAD] Filas rechazadas   : {rows_rejected}")
        if new_cols:
            logging.info(f"[OBSERVABILIDAD] Columnas nuevas    : {', '.join(new_cols)}")
        logging.info("=" * 60)

        # Publicar resumen en XCom para trazabilidad downstream
        ti.xcom_push(key='load_summary', value={
            'table': self.table_name,
            'rows_processed': rows_processed,
            'rows_rejected': rows_rejected,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration_seconds,
            'new_schema_columns': new_cols,
        })

        return rows_processed
