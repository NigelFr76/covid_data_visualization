# COVID-19 Big Data Analytics and Visualization Platform

An enterprise-grade, academic big data pipeline that ingests Our World in Data (OWID) COVID-19 records, processes them with **Apache Spark (PySpark)**, streams them via **Apache Kafka + Spark Structured Streaming**, stores structured analytics in **PostgreSQL**, serves data through a **FastAPI** REST interface, and visualizes real-time metrics in an interactive **Grafana** dashboard.

---

## 🏛️ Architecture Overview

```
                                      ┌────────────────────────────────┐
                                      │  Our World in Data (OWID) CSV  │
                                      │    (~429k historical rows)     │
                                      └──────────────┬─────────────────┘
                                                     │
                                                     ▼
                                      ┌────────────────────────────────┐
                                      │ Ingestion / Cleaning Pipeline  │
                                      │   (Validation, Restatements)   │
                                      └───────┬────────────────┬───────┘
                                              │                │
                      ┌───────────────────────┘                └────────────────────────┐
                      ▼                                                                 ▼
      ┌──────────────────────────────┐                                  ┌──────────────────────────────┐
      │   PySpark Batch Processing   │                                  │   Kafka Producer (Replay /   │
      │  (Window Aggregations, CFR)  │                                  │     Synthetic Generator)     │
      └───────┬──────────────┬───────┘                                  └──────────────┬───────────────┘
              │              │                                                         │
              ▼              ▼                                                         ▼
        Parquet Files   PostgreSQL DB                                           Kafka Topic:
      (Cleaned & Aggs)  (Relational DW)                                      `covid-raw-records`
                             ▲                                                         │
                             │                                                         ▼
                             │                                          ┌──────────────────────────────┐
                             └──────────────────────────────────────────┤   Spark Structured Streaming │
                                                                        │    (Micro-Batch Ingestion)   │
                                                                        └──────────────────────────────┘
                             ▲
                             │
            ┌────────────────┴────────────────┐
            │                                 │
            ▼                                 ▼
┌──────────────────────────────┐   ┌──────────────────────────────┐
│  Grafana Analytics Dashboard │   │     FastAPI REST Service     │
│   (14 Panels, Read-Only SQL) │   │  (Health, Replay & Summary)  │
│    http://localhost:3002     │   │    http://localhost:8001     │
└──────────────────────────────┘   └──────────────────────────────┘
```

### Data Source Classification

| `source_type` | Description | Default in Grafana |
|---|---|---|
| `historical` | Official cleaned Our World in Data dataset | **Yes** |
| `historical_replay` | Historical records re-streamed through Kafka | No (Selectable) |
| `synthetic` | Controlled synthetic demonstration data stream | No (Selectable) |

---

## 📋 Prerequisites

Before running the platform, ensure the following are installed:

1. **Docker & Docker Compose**: Docker Desktop v20+ or Docker Engine.
2. **Python**: Python 3.11, 3.12, or 3.13.
3. **Java 17+ (JDK)**: Required for running Apache Spark / PySpark jobs locally.
   - On macOS: `brew install openjdk@17`
   - Set environment variable: `export JAVA_HOME="/opt/homebrew/opt/openjdk@17"`

---

## 🚀 Quick Start & Local Setup

### Step 1: Clone and Enter Repository

```bash
git clone https://github.com/NigelFr76/covid_data_visualization.git
cd covid_data_visualization
```

### Step 2: Set Up Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On macOS / Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Configure Environment Variables

The project includes an environment template. Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

Default non-conflicting port mappings in `.env`:
- **PostgreSQL**: `localhost:5433`
- **Grafana**: `http://localhost:3002`
- **FastAPI**: `http://localhost:8001`
- **Kafka Broker**: `localhost:29092`
- **Zookeeper**: `localhost:22181`

### Step 4: Start Infrastructure Containers

Launch PostgreSQL, Apache Kafka, Zookeeper, and Grafana in Docker:

```bash
docker compose up -d postgres kafka zookeeper grafana
```

Verify that all containers are healthy:

```bash
docker compose ps
```

### Step 5: Initialize the Database Schema

Apply the database DDL and migrations (`init.sql` + `migrate.sql`):

```bash
python scripts/apply_schema.py
```

*Output:* `Schema applied.`

---

## 📊 Running Data Pipelines

### 1. Ingest Historical Data into PostgreSQL

Ingest the offline fixture (fast demonstration, 30 rows):
```bash
python -m ingestion.ingest_to_postgres --fixture
```

Or ingest the full Our World in Data historical CSV (~429k records):
```bash
python -m ingestion.ingest_to_postgres
```

### 2. Run Apache Spark Batch Analytics

Execute the distributed PySpark batch job to calculate 7-day rolling averages, Case Fatality Ratios (CFR), and regional trend increase signals:

```bash
# Set JAVA_HOME if not already in your shell
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"

# Run batch processing on test fixture
python -m spark.batch.process_historical --fixture

# Or run on full historical dataset
python -m spark.batch.process_historical
```

This writes:
- Optimized columnar Parquet files to `data/parquet/cleaned` and `data/parquet/aggregates`
- Upserts enriched records into `raw_covid_data`, `cleaned_covid_data`, `regional_aggregates`, and `regional_trend_signals`.

### 3. Verify Database Records

Print verification counts across all tables:

```bash
python scripts/verify_db.py
```

---

## 📈 Interactive Grafana Dashboard

Once data is ingested, access the pre-configured Grafana dashboard:

- **URL**: [http://localhost:3002/d/covid_main/covid-19-analytics-platform](http://localhost:3002/d/covid_main/covid-19-analytics-platform)
- **Direct Login**: `admin` / `admin` *(Click **Skip** if prompted to reset password)*
- **Dashboard Name**: **COVID-19 Analytics Platform**

### Dashboard Features & Panels (14 Interactive Visualizations):
1. **Total reported cases & deaths stat cards**
2. **Latest available reporting date counter**
3. **Daily new cases & daily new deaths time series**
4. **Seven-day rolling average trends** (cases and deaths smoothed)
5. **Country-wise multi-series comparison** (US, India, UK, France, Germany, Italy, Brazil, Japan)
6. **Case Fatality Ratio (CFR %)** ranking table
7. **Regional Increase Signals** (recent 7d vs preceding 7d percentage change)
8. **Detailed country-date observations table**
9. **Pipeline Health & Quarantine error tracking table**

---

## ⚡ Streaming & Real-Time Replay

### 1. Kafka Producer

Publish records into the `covid-raw-records` topic:

```bash
# Historical replay mode:
python -m producer.kafka_producer --mode historical --limit 100 --speed 0.02

# Synthetic real-time simulation mode:
python -m producer.kafka_producer --mode synthetic --limit 25 --speed 0.1
```

### 2. Spark Structured Streaming Consumer

Start the streaming consumer micro-batch engine:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
python -m spark.streaming.process_streaming
```

---

## 🌐 FastAPI REST Backend

The platform exposes REST APIs for pipeline health, data summary, and replay triggers:

### Running the API:
```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

Or run via Docker:
```bash
docker compose up -d --build api
```

### Endpoints:
- **Interactive Swagger UI**: [http://localhost:8001/docs](http://localhost:8001/docs)
- `GET /health` - Service health status & database connectivity
- `GET /api/summary?source_type=historical` - Aggregated worldwide statistics
- `GET /api/trends?location=United%20States` - Smoothed trend data
- `GET /api/regions` - Regional increase statistical signals
- `POST /api/pipeline/replay/start` - Initiate automated Kafka message replay
- `POST /api/pipeline/replay/stop` - Halt replay execution

---

## 🧪 Automated Testing

Run the comprehensive unit, Spark transform, API, and PostgreSQL integration test suite:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
pytest tests -v
```

All 26 tests covering data cleaning, revision handling, CFR calculation, Kafka serialization, and Spark DataFrame transformations should pass.

---

## 🧹 Resetting Demo Data

To reset streaming, synthetic, and test rows while keeping historical OWID data:

```bash
python scripts/reset_demo.py
```

To perform a clean tear-down of all Docker containers and volumes:

```bash
docker compose down -v
```

---

## 🛠️ Project Directory Structure

```
covid_data_visualization/
├── analytics/                  # Core analytics & statistical formulas
│   ├── metrics.py              # Rolling mean, CFR %, percentage change, trend signals
│   └── trend_analysis.py       # SQL loader & regional increase signal detection
├── api/                        # FastAPI service
│   └── main.py                 # REST endpoints and Kafka replay controllers
├── config/                     # Configuration management
│   └── settings.py             # Dynamic environment loader & database connection strings
├── data/                       # Data assets & Parquet storage
│   ├── fixtures/covid_small.csv# 30-row deterministic offline testing fixture
│   └── owid-covid-data.csv     # Full OWID historical CSV dataset
├── grafana/                    # Grafana configuration
│   ├── dashboards/covid.json   # Full 14-panel dashboard JSON definition
│   └── provisioning/           # Automated datasource & dashboard provisioning
├── ingestion/                  # Data ingestion & cleaning
│   ├── cleaning.py             # Validation, deduplication & cumulative restatement logic
│   ├── db.py                   # Parameterized PostgreSQL batch upserts
│   ├── download_data.py        # Automated OWID downloader with caching
│   ├── ingest_to_postgres.py   # Standalone Python ingestion script
│   └── schema.py               # Schema constraints & column definitions
├── producer/                   # Apache Kafka event producer
│   ├── kafka_producer.py       # Historical replay & synthetic event generator
│   └── messages.py             # Message builders & JSON serializers
├── scripts/                    # Database management scripts
│   ├── apply_schema.py         # DDL initialization script
│   ├── reset_demo.py           # Demo/synthetic cleanup script
│   └── verify_db.py            # Table count & integrity verifier
├── spark/                      # Apache Spark engine
│   ├── batch/                  # PySpark historical batch jobs & transforms
│   ├── schemas.py              # PySpark StructType schemas
│   └── streaming/              # Spark Structured Streaming consumers & microbatches
├── sql/                        # SQL schemas
│   ├── init.sql                # Base DDL & user creation
│   └── migrate.sql             # Idempotent migration scripts
├── tests/                      # Automated test suite
│   ├── test_api.py             # FastAPI endpoint tests
│   ├── test_e2e.py             # End-to-end PostgreSQL integration tests
│   ├── test_pipeline.py        # Cleaning, metrics & message unit tests
│   └── test_spark_transforms.py# PySpark batch and streaming DataFrame tests
├── docker-compose.yml          # Multi-container Docker Compose configuration
├── Dockerfile                  # Production container for FastAPI backend
├── Dockerfile.spark            # Spark container with psycopg2 & dependencies
├── requirements.txt            # Python dependencies (Python 3.11-3.13 compatible)
└── README.md                   # Project documentation
```

---

## 👨‍💻 Author

- **Name**: Nigel Francy
- **PRN**: 23070122148
- **Department**: Department of Computer Science and Engineering
- **Institute**: Symbiosis Institute of Technology (SIT), Pune
- **Course**: Big Data Analytics (CA3)
