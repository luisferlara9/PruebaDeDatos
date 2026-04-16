import csv
import random
from datetime import datetime
import os

def generate_telemetry_csv(file_path, num_records=1000):
    """
    Genera un archivo CSV de telemetría simulado sin usar externos (solo librerías estándar).
    """
    device_ids = [f"DEV-{i:03d}" for i in range(1, 21)]
    headers = ['id', 'device_id', 'timestamp', 'metric_value', 'status']
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for i in range(1, num_records + 1):
            # Simulamos algunos nulos (aprox 2% de forma saludable)
            device_id = random.choice(device_ids) if random.random() > 0.02 else ""
            
            row = {
                'id': i,
                'device_id': device_id,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'metric_value': float(f"{random.uniform(20.0, 100.0):.2f}"),
                'status': random.choices(['OK', 'ERROR', 'WARN'], weights=[80, 10, 10])[0]
            }
            writer.writerow(row)
            
    print(f"Exito: Se generaron {num_records} registros en {file_path}")

if __name__ == "__main__":
    generate_telemetry_csv('pipeline/data/telemetry_logs.csv')
