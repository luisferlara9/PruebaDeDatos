# Solución de la Prueba Técnica: Senior Data Engineer

Este repositorio contiene la solución para la prueba técnica de Senior Data Engineer, enfocándose en la Arquitectura Medallion, Orquestación con Airflow y Observabilidad de Datos.

## Estructura del Proyecto

- `architecture/`: Parte 1 - Diseño de Arquitectura y Justificación.
- `pipeline/`: Parte 2 - Implementación Técnica (DAG de Airflow, Operadores Personalizados, Datos Simulados).
- `leadership/`: Parte 3 - Liderazgo, SLAs y Guías de Revisión de Código.

## Configuración y Configuración

### Requisitos Previos
- Python 3.9+
- Airflow 2.5+ (Recomendado)
- Postgres DWH

### Inicio Rápido (Datos Simulados)
```bash
python pipeline/scripts/generate_data.py
```

## Puntos Destacados de la Solución
- **Capas Medallion**: Separación clara entre Bronze (Staging), Silver (Fact/Dims) y Gold (Agregados).
- **Idempotencia**: Lógica UPSERT implementada en un operador de Postgres personalizado.
- **Calidad de Datos**: Verificación automatizada con un umbral de error del 5%.
- **Observabilidad**: Registro basado en métricas y configuración de alertas de SLA.
