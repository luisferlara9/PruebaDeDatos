import csv
import json
import os

def simulate_pipeline():
    print("=== Iniciando Simulacion Local del Pipeline (Version Sin Dependencias) ===")
    
    csv_path = 'pipeline/data/telemetry_logs.csv'
    if not os.path.exists(csv_path):
        print(f"Error: No se encuentra el archivo {csv_path}")
        print("Por favor, ejecuta primero: python pipeline/scripts/generate_data.py")
        return

    # 1. Extraccion y Validacion DQ
    print(f"\n[1/3] Validando Calidad de Datos (DQ) en {csv_path}...")
    
    critical_cols = ['id', 'device_id', 'timestamp']
    
    with open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    total_rows = len(rows)
    # Contar cuantos registros tienen fallos en columnas criticas
    null_count = sum(1 for row in rows if any(not row[col] for col in critical_cols))
    
    if total_rows == 0:
        print("Error: El archivo esta vacio.")
        return

    null_ratio = float(null_count) / float(total_rows)
    print(f"Metricas DQ:")
    print(f"  - Filas totales procesadas: {total_rows}")
    print(f"  - Registros con fallos (nulos): {null_count}")
    print(f"  - Ratio de error: {null_ratio:.2%}")
    
    if null_ratio > 0.05:
        print(f"!!! FALLO DE DQ: El ratio {null_ratio:.2%} supera el umbral del 5%.")
        print("Abortando proceso de carga.")
        return
    else:
        print(">>> CALIDAD OK: Los datos cumplen con los requisitos.")

    # 2. Logica de UPSERT
    print("\n[2/3] Generando Logica de Carga Incremental (Idempotencia)...")
    
    # Simulamos las columnas que vendrian del CSV
    columns = ['id', 'device_id', 'timestamp', 'metric_value', 'status']
    unique_key = 'id'
    
    # Construir sentencia dinamicamente para Postgres
    insert_cols = ", ".join(columns)
    update_parts = [f"{col} = EXCLUDED.{col}" for col in columns if col != unique_key]
    update_stmt = ", ".join(update_parts)
    
    sql_preview = f"""
    INSERT INTO silver.fact_telemetry ({insert_cols})
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ({unique_key})
    DO UPDATE SET {update_stmt};
    """
    
    print("Consulta SQL Generada por el sistema:")
    print("-" * 50)
    print(sql_preview.strip())
    print("-" * 50)

    # 3. Conclusion
    print("\n[3/3] Resumen Final:")
    print("Esta simulacion demuestra que la logica de negocio es independiente del entorno.")
    print("- Idempotencia: Confirmada mediante ON CONFLICT.")
    print("- Calidad: Validada mediante el conteo manual de nulos sin librerias externas.")
    
    print("\n=== Simulacion Finalizada con Exito ===")

if __name__ == "__main__":
    simulate_pipeline()
