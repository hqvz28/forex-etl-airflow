# Automated Forex ETL Pipeline with Apache Airflow & Docker

[![Python](https://img.shields.io/badge/Python-3.7%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.5.1-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Docker%20Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot%20API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots/api)

An end-to-end automated Data Engineering ETL pipeline orchestrating daily foreign exchange (Forex) currency rate ingestion, PostgreSQL database storage with idempotent upserts, time-series volatility analysis, and automated Telegram reporting using **Apache Airflow** and **Docker Compose**.

---

## 📌 Table of Contents
- [Architecture & Workflow](#-architecture--workflow)
- [Key Features](#-key-features)
- [DAG Tasks Breakdown](#-dag-tasks-breakdown)
- [Tech Stack](#-tech-stack)
- [Project Directory Structure](#-project-directory-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Backfill Historical Data](#backfill-historical-data)
- [Pipeline Execution Previews](#-pipeline-execution-previews)
- [Author](#-author)

---

## 🏗 Architecture & Workflow

The pipeline runs on a scheduled basis (`0 1 * * *` - Daily at 01:00 UTC) with catchup enabled for historical backfills:

```
+--------------------------------------------------------------------------------------------------+
|                                  Automated Forex ETL Pipeline                                    |
+--------------------------------------------------------------------------------------------------+

   [ APILayer Exchange Rates API ]
             |
             | (1) HTTP GET Daily Currency Rates (Base: USD)
             v
   +-------------------------------+
   |  fetch_exchange_rate_data     |  --> Saves raw JSON: /opt/airflow/data/exchange_rates_{{ ds }}.json
   +-------------------------------+
             |
             v
   +-------------------------------+
   |        create_table           |  --> DDL: CREATE TABLE IF NOT EXISTS exchange_rates (PostgreSQL)
   +-------------------------------+
             |
             v
   +-------------------------------+
   |      insert_upsert_data       |  --> Idempotent SQL Upsert: ON CONFLICT (date, base, currency)
   +-------------------------------+      DO UPDATE SET rate = EXCLUDED.rate
             |
             v
   +-------------------------------+
   |     analyze_exchange_rates    |  --> Queries history, computes max rate deviation vs current date,
   +-------------------------------+      exports findings to /opt/airflow/data/max_deviation.csv
             |
             v
   +-------------------------------+
   |     send_csv_to_telegram      |  --> Telegram Bot API pushes alert & CSV report directly to chat
   +-------------------------------+

+--------------------------------------------------------------------------------------------------+
| Environment: Apache Airflow 2.5.1 (CeleryExecutor) | PostgreSQL 13 | Redis Broker | Docker Compose |
+--------------------------------------------------------------------------------------------------+
```

---

## 🚀 Key Features

- **Robust Ingestion**: Pulls foreign currency rates against base USD for 150+ international currencies using APILayer Forex API.
- **Data Idempotency & ACID Compliance**: Uses PostgreSQL composite primary keys `(date, base, currency)` with `ON CONFLICT DO UPDATE` ensuring fault tolerance on re-runs and historical backfills.
- **Statistical Volatility Analysis**: Analyzes historical rate trends to find the peak deviation date and value for every currency relative to the current execution date.
- **Automated Alerting**: Sends synthesized CSV reports with personalized notification captions to Telegram via bot webhook.
- **Containerized Orchestration**: Fully packaged with CeleryExecutor, Redis, PostgreSQL, and Airflow Webserver/Scheduler using Docker Compose.

---

## 🔍 DAG Tasks Breakdown

| Task ID | Operator | Description |
| :--- | :--- | :--- |
| `fetch_exchange_rate_data` | `PythonOperator` | Ingests daily exchange rates from APILayer REST API for the execution date `{{ ds }}` and writes raw payload to JSON in shared volume. |
| `create_table` | `PostgresOperator` | Idempotently initializes the relational `exchange_rates` schema in PostgreSQL with composite primary key `(date, base, currency)`. |
| `insert_upsert_data` | `PythonOperator` | Transforms raw JSON records using Pandas and executes batch upsert via `PostgresHook` into PostgreSQL. |
| `analyze_exchange_rates` | `PythonOperator` | Pulls historical currency rates, computes absolute deviation `|rate - current_rate|` for all dates up to execution date, identifies max deviation, and saves summary to `max_deviation.csv`. |
| `send_csv_to_telegram` | `PythonOperator` | Dispatches the generated CSV report and alert message to Telegram channel using the Telegram Bot API `sendDocument` endpoint. |

---

## 🛠 Tech Stack

- **Workflow Orchestration**: [Apache Airflow](https://airflow.apache.org/) (CeleryExecutor)
- **Programming Language**: [Python 3.7+](https://www.python.org/) (Pandas, Requests, SQLAlchemy, psycopg2)
- **Database & Storage**: [PostgreSQL 13](https://www.postgresql.org/), Local Docker Volumes
- **Message Broker & Queue**: [Redis](https://redis.io/)
- **Infrastructure**: [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- **Alerting & Notification**: [Telegram Bot API](https://core.telegram.org/bots/api)
- **External Data Source**: [APILayer Exchange Rates Data API](https://apilayer.com/marketplace/exchangerates_data-api)

---

## 📁 Project Directory Structure

```bash
forex-etl-airflow/
├── dags/
│   └── forex_etl_pipeline.py    # Main Airflow DAG definition and task logic
├── data/                        # Shared runtime data directory (mounted volume)
├── docker-compose.yaml          # Multi-container orchestration (Airflow, Postgres, Redis)
├── .env.example                 # Template for required environment variables
├── .gitignore                   # Standard Git ignore rules for Python, Docker & Airflow
└── README.md                    # Project documentation
```

---

## ⚡ Getting Started

### Prerequisites
- [Docker Engine](https://docs.docker.com/engine/install/) (v20.10.0+)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2.0.0+)
- 4GB+ RAM allocated to Docker daemon

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

3. Ensure Airflow variables (API keys and Telegram tokens) are specified in `.env` or set inside the Airflow UI / `docker-compose.yaml`:
   - `AIRFLOW_VAR_API_KEY`: APILayer Exchange Rates API key
   - `AIRFLOW_VAR_BOT_TOKEN`: Telegram Bot authorization token
   - `AIRFLOW_VAR_CHAT_ID`: Telegram target chat ID
   - `AIRFLOW_VAR_SYMBOLS`: Comma-separated currency symbols list

### Running with Docker Compose
1. Initialize the Airflow database and create default admin credentials:
   ```bash
   docker compose up airflow-init
   ```

2. Start all services in detached mode:
   ```bash
   docker compose up -d
   ```

3. Access the Airflow Web UI:
   - URL: `http://localhost:8080`
   - Default Username: `airflow`
   - Default Password: `airflow`

4. Unpause the `forex_etl_pipeline` DAG in the UI to trigger scheduled runs.

### Backfill Historical Data
To trigger backfilling historical forex rates starting from `2024-09-01`:
```bash
docker compose exec airflow-webserver airflow dags backfill \
    --start-date 2024-09-01 \
    --end-date 2024-09-30 \
    forex_etl_pipeline
```

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
