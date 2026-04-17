# 🏗️ Senior Data Engineer — Prueba Técnica

Solución completa para el Technical Assessment de **Senior Data Engineer**, implementando una arquitectura de datos de nivel producción sobre un Data Warehouse en Postgres, orquestada con Apache Airflow.

---

## 📋 Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura](#arquitectura)
- [Estructura del Repositorio](#estructura-del-repositorio)
- [Cobertura de Criterios de Evaluación](#cobertura-de-criterios-de-evaluación)
- [Instalación y Configuración](#instalación-y-configuración)
- [Ejecución Local (Sin Dependencias)](#ejecución-local-sin-dependencias)
- [Despliegue en Airflow](#despliegue-en-airflow)
- [Variables de Entorno](#variables-de-entorno)
- [Documentación Detallada](#documentación-detallada)

---

## Descripción General

La compañía está migrando sus procesos de BI hacia un **Data Warehouse centralizado en Postgres**. Este repositorio implementa el pipeline de ingesta completo para logs de telemetría provenientes de almacenamiento en la nube (S3/Blob Storage).

**Tecnologías utilizadas**: Python 3.9+, Apache Airflow 2.5+, PostgreSQL 13+, Pandas.

---

## Arquitectura

El pipeline sigue la **Arquitectura Medallion** de tres capas:

```
S3 / Blob Storage (CSV/JSON)
        │
        ▼  [Airflow DAG: telemetry_ingestion_v2]
┌───────────────────────────────────────────────┐
│  BRONZE  │  stg_telemetry_logs  │  Dato crudo │
├───────────────────────────────────────────────┤
│  SILVER  │  fact_telemetry      │  Limpio + UPSERT idempotente │
├───────────────────────────────────────────────┤
│  GOLD    │  agg_device_usage_daily │  Agregados para Power BI  │
└───────────────────────────────────────────────┘
```

Para el diagrama completo en Mermaid, ver [`architecture/design.md`](architecture/design.md).

---

## Estructura del Repositorio

```
.
├── README.md                        # Este archivo
├── requirements.txt                 # Dependencias Python del proyecto
├── DOCUMENTACION_TECNICA.md         # Documentación técnica exhaustiva
├── .gitignore
│
├── architecture/
│   └── design.md                    # Parte 1: Diagrama Mermaid + justificación técnica
│
├── leadership/
│   └── guidelines.md                # Parte 3: SLAs, Observabilidad y Code Review
│
└── pipeline/
    ├── dags/
    │   └── telemetry_ingestion_dag.py   # DAG principal de Airflow
    │
    ├── plugins/
    │   └── operators/
    │       └── postgres_upsert_operator.py  # Operador UPSERT personalizado con evolución de esquema
    │
    ├── scripts/
    │   ├── generate_data.py         # Generador de datos sintéticos (1000 registros)
    │   └── test_local.py            # Simulación del pipeline sin dependencias externas
    │
    └── data/
        └── telemetry_logs.csv       # Generado por generate_data.py (no versionado)
```

---

## Cobertura de Criterios de Evaluación

| Criterio | Estado | Implementado en |
|---|:---:|---|
| Arquitectura Medallion (Bronze/Silver/Gold) | ✅ | `architecture/design.md` · `telemetry_ingestion_dag.py` |
| Idempotencia con UPSERT (`ON CONFLICT`) | ✅ | `postgres_upsert_operator.py` |
| Manejo de Esquemas Dinámicos (`ALTER TABLE`) | ✅ | `_ensure_schema_evolution()` en el operador |
| Observabilidad: Tiempo inicio/fin | ✅ | Logging en ambas tareas del DAG |
| Observabilidad: Filas procesadas | ✅ | `PostgresUpsertOperator` |
| Observabilidad: Filas rechazadas por DQ | ✅ | `extract_and_validate_dq()` |
| Validación DQ con fallo explícito si >5% nulos | ✅ | `raise ValueError` en el DAG |
| SLA Airflow (6:00 AM) + callback de alerta | ✅ | `sla_miss_callback` · `timedelta(hours=4)` |
| Guía de Code Review para DE Junior | ✅ | `leadership/guidelines.md` |
| Particionamiento lógico para 50M registros | ✅ | `architecture/design.md` (sección 2) |

---

## Instalación y Configuración

### Requisitos Previos
- Python **3.9** o superior
- pip / virtualenv
- PostgreSQL **13** o superior (para ejecución con Airflow)
- Apache Airflow **2.5** o superior

### 1. Clonar el repositorio
```bash
git clone <url-del-repositorio>
cd Prueba
```

### 2. Crear entorno virtual
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux / macOS
source venv/bin/activate
```

### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

---

## Ejecución Local (Sin Dependencias)

Para validar la lógica del pipeline sin necesitar Airflow ni Postgres instalados:

```bash
# Paso 1: Generar el dataset sintético de telemetría
python pipeline/scripts/generate_data.py
# ► Exito: Se generaron 1000 registros en pipeline/data/telemetry_logs.csv

# Paso 2: Simular el pipeline completo (validación DQ + lógica UPSERT)
python pipeline/scripts/test_local.py
```

**Salida esperada de `test_local.py`:**
```
=== Iniciando Simulacion Local del Pipeline ===

[1/3] Validando Calidad de Datos (DQ)...
  - Filas totales procesadas: 1000
  - Registros con fallos (nulos): 14
  - Ratio de error: 1.40%
>>> CALIDAD OK: Los datos cumplen con los requisitos.

[2/3] Generando Logica de Carga Incremental (Idempotencia)...
INSERT INTO silver.fact_telemetry (...)
ON CONFLICT (id) DO UPDATE SET ...;

[3/3] Resumen Final: Simulacion Finalizada con Exito
```

---

## Despliegue en Airflow

### 1. Copiar archivos al entorno Airflow
```bash
# Copiar el DAG
cp pipeline/dags/telemetry_ingestion_dag.py $AIRFLOW_HOME/dags/

# Copiar el operador personalizado
cp -r pipeline/plugins/ $AIRFLOW_HOME/plugins/
```

### 2. Configurar la conexión a Postgres en Airflow
En la UI de Airflow → **Admin → Connections → Add Connection**:

| Campo | Valor |
|---|---|
| Conn Id | `postgres_dwh` |
| Conn Type | `Postgres` |
| Host | `<host-de-tu-postgres>` |
| Schema | `silver` |
| Login | `<usuario>` |
| Password | `<contraseña>` |
| Port | `5432` |

### 3. Activar el DAG
En la UI de Airflow, buscar `telemetry_ingestion_v2` y activar el toggle.

---

## Variables de Entorno

| Variable | Descripción | Valor por defecto |
|---|---|---|
| `TELEMETRY_CSV_PATH` | Ruta al archivo CSV de origen | `pipeline/data/telemetry_logs.csv` |
| `AIRFLOW_HOME` | Directorio raíz de Airflow | `~/airflow` |

---

## Documentación Detallada

Para una explicación exhaustiva de todas las decisiones de diseño, ver:

📄 **[`DOCUMENTACION_TECNICA.md`](DOCUMENTACION_TECNICA.md)** — Cubre:
- Justificación de la Arquitectura Medallion
- Estrategia de Carga Incremental para 50M registros/mes
- Implementación de Idempotencia y Evolución de Esquema
- Sistema de Observabilidad y formato de logs
- Guías de Liderazgo Técnico y Code Review
