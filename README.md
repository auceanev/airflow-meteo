# Airflow Météo Pipeline

Pipeline ETL orchestré avec Apache Airflow qui collecte les données météo
horaires de Paris via l'API Open Meteo et les stocke dans SQLite.

## Architecture

```
API Open Meteo → [extract] → [transform] → [load] → SQLite
↑            ↑           ↑
Airflow DAG (orchestration toutes les heures)
```
## Stack

- Apache Airflow 2.9
- Docker & Docker Compose
- Python / pandas / requests
- PostgreSQL (metadata Airflow)
- SQLite (stockage données)

## Lancer le projet

```bash
mkdir -p dags logs plugins
chmod -R 777 logs dags plugins
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler postgres
```

Ouvre http://localhost:8080 — login: `admin` / password: `admin`

## Structure
