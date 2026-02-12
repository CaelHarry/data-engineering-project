# SaaS Analytics Data Pipeline

An end-to-end batch analytics pipeline that ingests raw event data, transforms it using dbt, and orchestrates daily runs with Airflow. This project models product usage metrics such as DAU, signups, and feature engagement, delivering analytics-ready data for dashboards and reporting.

## Overview

This project simulates a production-grade SaaS analytics environment, implementing a modern data stack similar to what data teams use in practice. The pipeline ingests raw user activity events, stores them in PostgreSQL, applies transformations via dbt, and orchestrates the entire workflow with Airflow to produce dashboard-ready KPI metrics including DAU, engagement, and signups.

## Architecture

### Stack
- **PostgreSQL** – Data warehouse
- **dbt** – Data transformations & modeling
- **Apache Airflow** – Orchestration
- **Docker** – Containerized environment

### Pipeline Flow
```
Raw Events → PostgreSQL (raw layer) → dbt staging models → 
dbt fact tables (analytics layer) → Airflow scheduled execution
```

## Data Model

### `fact_events`
One row per event, serving as the source of truth for user behavior.
- `user_id`
- `event_type`
- `event_timestamp`

### `fact_daily_events`
One row per day with aggregated KPIs:
- `total_events`
- `daily_active_users`
- `events_per_user`
- `signup_events`
- Feature usage counts

This two-tier model enables flexible user-level analysis while supporting fast dashboard queries without heavy joins.

## Key Features

**Incremental dbt Models**  
Uses `materialized='incremental'` to reprocess only recent days, handling late-arriving data efficiently while reducing compute compared to full refresh.

**Airflow Orchestration**  
DAG executes dbt staging and mart models with task dependency management, logging, and automatic retries on failure.

**Containerized Environment**  
Docker Compose configuration with isolated services for Airflow and PostgreSQL, enabling consistent development and deployment.

**Data Quality Foundations**  
dbt tests ensure data integrity with `not_null` and `unique` constraints, supported by a schema-driven modeling approach.

## Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.9+

### Installation
```bash
git clone https://github.com/yourusername/saas-analytics-pipeline
cd saas-analytics-pipeline

# Start services
docker-compose up -d

# Initialize Airflow
docker-compose exec airflow airflow db init

# Run dbt models
docker-compose exec airflow dbt run --project-dir /path/to/dbt

# Access Airflow UI at http://localhost:8080
```

## Usage

The pipeline runs daily via Airflow, executing the full transformation workflow from raw events to analytics-ready fact tables. To trigger manually:
```bash
airflow dags trigger saas_analytics_pipeline
```

## License

MIT