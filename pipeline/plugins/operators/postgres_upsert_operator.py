from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import logging

class PostgresUpsertOperator(BaseOperator):
    """
    Operador personalizado para realizar operaciones UPSERT (INSERT ON CONFLICT) en Postgres.
    Garantiza la idempotencia para la misma partición lógica.
    """
    
    @apply_defaults
    def __init__(
        self,
        table_name,
        postgres_conn_id='postgres_default',
        unique_key='id',
        df=None,
        *args, **kwargs
    ):
        super(PostgresUpsertOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.unique_key = unique_key
        self.df = df

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        if self.df is None:
            logging.info("No hay datos para cargar.")
            return 0

        # Evolución Dinámica de Esquema: Asegurar que las columnas existen en la tabla destino
        # (En un escenario real, verificaríamos la meta de la tabla y usaríamos ALTER TABLE si es necesario)
        
        cols = ",".join([str(i) for i in self.df.columns.tolist()])
        
        # Construir consulta UPSERT
        update_cols = [f"{col} = EXCLUDED.{col}" for col in self.df.columns if col != self.unique_key]
        update_stmt = ", ".join(update_cols)
        
        sql = f"""
            INSERT INTO {self.table_name} ({cols})
            VALUES %s
            ON CONFLICT ({self.unique_key})
            DO UPDATE SET {update_stmt};
        """
        
        values = [tuple(x) for x in self.df.to_numpy()]
        
        start_time = datetime.now()
        hook.insert_rows(
            table=self.table_name,
            rows=values,
            target_fields=self.df.columns.tolist(),
            commit_every=1000,
            replace=True, # Esto usa ON CONFLICT si el hook lo soporta o SQL personalizado
            replace_index=self.unique_key
        )
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        rows_processed = len(self.df)
        
        # Registrar métricas de observabilidad
        logging.info(f"Resumen de Ejecución:")
        logging.info(f"Tabla: {self.table_name}")
        logging.info(f"Filas Procesadas: {rows_processed}")
        logging.info(f"Duración: {duration}s")
        
        return rows_processed
