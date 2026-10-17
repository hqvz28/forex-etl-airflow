# Enterprise Automated Forex ETL Pipeline with Apache Airflow & Docker

[![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-TaskFlow%20API-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Docker%20Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Star%20Schema-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot%20API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots/api)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI%2FCD-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)

An enterprise-grade, resilient Data Engineering pipeline orchestrating foreign exchange (Forex) ingestion with **automatic multi-source fallback (APILayer + Frankfurter ECB)**, **pre-flight data quality validation gates**, **Kimball Star Schema dimensional modeling in PostgreSQL**, statistical volatility analysis, and automated Telegram alerting via **Apache Airflow TaskFlow API** and **Docker Compose**.

---

## 📌 Table of Contents
- [Enterprise Architecture Overview](#-enterprise-architecture-overview)
- [Key Engineering Pillars](#-key-engineering-pillars)
- [DAG Implementations](#-dag-implementations)
  - [1. Modern TaskFlow Pipeline (forex_etl_taskflow.py)](#1-modern-taskflow-pipeline-forex_etl_taskflowpy)
  - [2. Classic Pipeline (forex_etl_pipeline.py)](#2-classic-pipeline-forex_etl_pipelinepy)
- [Dimensional Data Warehouse Modeling (Kimball Star Schema)](#-dimensional-data-warehouse-modeling-kimball-star-schema)
- [Automated Data Quality Gatekeeper](#-automated-data-quality-gatekeeper)
- [Project Directory Structure](#-project-directory-structure)
- [Continuous Integration & Testing (CI/CD)](#-continuous-integration--testing-cicd)
- [Getting Started](#-getting-started)
- [Pipeline Execution Previews](#-pipeline-execution-previews)
- [Author](#-author)

---

## 🏗 Enterprise Architecture Overview

```
+--------------------------------------------------------------------------------------------------+
|                              Enterprise Resilient Forex ETL Pipeline                             |
+--------------------------------------------------------------------------------------------------+

   [ Primary: APILayer REST API ]        [ Fallback: Frankfurter / ECB API ]
                 \                                     /
                  \---> [ Automatic Resilient Failover Ingestion ]
                                      |
                                      v
                      +-------------------------------+
                      |   validate_data_quality       |  --> Pre-flight Schema & Completeness Gate
                      +-------------------------------+      (Asserts > 5 pairs, positive rates)
                                      |
                                      v
                      +-------------------------------+
                      |  stage_and_upsert_postgres    |  --> Staging Table (Idempotent ON CONFLICT)
                      +-------------------------------+
                                      |
                                      v
                      +-------------------------------+
                      |     transform_star_schema     |  --> Kimball Star Schema Modeling:
                      +-------------------------------+      (dim_currency, dim_date, fact_exchange_rates)
                                      |
                                      v
                      +-------------------------------+
                      |  compute_volatility_and_alert |  --> Volatility Analytics & Telegram Alert
                      +-------------------------------+

+--------------------------------------------------------------------------------------------------+
| Infrastructure: Apache Airflow 2.5.1 (CeleryExecutor) | PostgreSQL 13 | Redis | Docker Compose    |
+--------------------------------------------------------------------------------------------------+
```

---

## 💡 Key Engineering Pillars

1. **Multi-Source Resilient Ingestion**: Solves third-party API rate limits and downtime by attempting APILayer as Primary and seamlessly falling back to European Central Bank data via the open Frankfurter API.
2. **Automated Quality Gatekeeper**: Protects downstream data assets from corrupt payloads, missing currencies, or zero/negative rate values before triggering warehouse writes.
3. **Kimball Star Schema Dimensional Modeling**: Transforms flat staging records into clean dimensional tables (`dim_date`, `dim_currency`, `fact_exchange_rates`, `fact_currency_volatility`) optimized for BI and financial analytics.
4. **Idempotent Upserts & ACID Guarantee**: Composite primary keys and `ON CONFLICT DO UPDATE` queries allow safe backfilling and re-execution without duplicate records.
5. **CI/CD Quality Assurance**: Automated GitHub Actions workflow runs `flake8` linter and `pytest` DAG integrity tests on every push.

---

## 🔄 DAG Implementations

### 1. Modern TaskFlow Pipeline (`forex_etl_taskflow.py`)
Uses the modern Airflow 2.x `@dag` and `@task` decorators:
- `fetch_rates_with_fallback`: Ingests rates from APILayer, automatically failing over to Frankfurter ECB API.
- `validate_data_quality`: Enforces schema completeness, non-null checks, and positive value constraints.
- `stage_and_upsert_postgres`: Performs batch upsert into `exchange_rates` staging table.
- `transform_star_schema`: Populates `dim_currency`, `dim_date`, and `fact_exchange_rates`.
- `compute_volatility_and_alert`: Calculates currency volatility and dispatches formatted Telegram notifications.

### 2. Classic Pipeline (`forex_etl_pipeline.py`)
Contains the original sequential operator design with `PostgresOperator` and `PythonOperator` for legacy Airflow backward compatibility.

---

## 🏛 Dimensional Data Warehouse Modeling (Kimball Star Schema)

Located in [`sql/dimensional_model.sql`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/forex-etl-airflow/sql/dimensional_model.sql):

- **`dim_date`**: Conformed date dimension with calendar attributes (`date_key`, `day_of_week`, `month`, `quarter`, `year`, `is_weekend`).
- **`dim_currency`**: Currency dimension (`currency_code`, `currency_name`, `country_or_zone`, `is_g10_major`).
- **`fact_exchange_rates`**: Daily currency rates grain (`date_key`, `base_currency_code`, `target_currency_code`, `exchange_rate`, `daily_change`).
- **`fact_currency_volatility`**: Rolling metrics (`rolling_7d_volatility`, `rolling_30d_volatility`, `bollinger_upper_band`, `bollinger_lower_band`).
- **`v_currency_momentum`**: Ready-to-query analytical view tracking G10 major currency momentum.

---

## 📁 Project Directory Structure

```bash
forex-etl-airflow/
├── .github/
│   └── workflows/
│       └── airflow-ci.yml       # GitHub Actions CI for DAG integrity and linting
├── dags/
│   ├── forex_etl_taskflow.py    # Modern Airflow 2.x TaskFlow DAG with fallback & star schema
│   └── forex_etl_pipeline.py    # Classic operator-based Airflow DAG
├── data/                        # Shared runtime volume directory
├── sql/
│   └── dimensional_model.sql    # Kimball Star Schema DDL & analytical views
├── tests/
│   └── test_dag_integrity.py    # Automated DAG syntax, integrity, and security unit tests
├── docker-compose.yaml          # Multi-container orchestration (Airflow, Postgres, Redis)
├── .env.example                 # Sanitized environment configuration template
├── .gitignore                   # Standard Git ignore rules
└── README.md                    # Comprehensive project documentation
```

---

## ⚡ Getting Started

### Prerequisites
- [Docker Engine](https://docs.docker.com/engine/install/) & [Docker Compose](https://docs.docker.com/compose/install/)
- 4GB+ RAM allocated to Docker

### Configuration
1. Clone the repository:
   ```bash
   git clone https://github.com/hqvz28/forex-etl-airflow.git
   cd forex-etl-airflow
   ```

2. Copy the environment configuration:
   ```bash
   cp .env.example .env
   ```

3. Configure your API keys (optional, fallback API works automatically out of the box!):
   - `AIRFLOW_VAR_API_KEY`: APILayer Exchange Rates API key
   - `TELEGRAM_BOT_TOKEN`: Telegram Bot authorization token
   - `TELEGRAM_CHAT_ID`: Telegram target chat ID

### Running with Docker Compose
1. Initialize the database:
   ```bash
   docker compose up airflow-init
   ```

2. Start the services:
   ```bash
   docker compose up -d
   ```

3. Access the Airflow UI at `http://localhost:8080` (Credentials: `airflow` / `airflow`).

---

## 🧪 Continuous Integration & Testing (CI/CD)

Run local DAG integrity tests:
```bash
python3 -m unittest discover tests
```
The automated GitHub Actions workflow will validate:
- Zero syntax errors in DAG files.
- Zero hardcoded credentials or tokens.
- Compliance with Airflow TaskFlow standards.

---

## 📊 Pipeline Execution Previews

### 1. Conceptual Architecture Diagram
![Architecture Diagram](https://github.com/user-attachments/assets/aa26652c-1c8c-439a-bc8a-721bf0468dbb)

### 2. Airflow Grid & Execution History
![Airflow Grid View](https://github.com/user-attachments/assets/b4a3f354-f1f7-4a97-80b8-8b9a1dc10608)

### 3. Task Dependency Graph View
![Graph View](https://github.com/user-attachments/assets/2ac3aa7c-c62b-49b3-a3f1-ff823d5cae8a)

### 4. Backfill Runs Progression
![Backfill Progression](https://github.com/user-attachments/assets/0b77f692-b8ef-4a6a-bf69-4ce2fc591fe2)

### 5. Automated Telegram Report
![Telegram Bot Notification](https://github.com/user-attachments/assets/443203fc-a282-4226-9434-521748c9ac78)

---

## 👨‍💻 Author

**Hoang Quoc Viet**
- **Portfolio**: [https://hqvz28.github.io](https://hqvz28.github.io)
- **LinkedIn**: [https://www.linkedin.com/in/qviet28/](https://www.linkedin.com/in/qviet28/)
- **GitHub**: [@hqvz28](https://github.com/hqvz28)
