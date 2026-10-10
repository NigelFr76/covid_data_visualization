# COVID-19 Big Data Analytics and Visualization Platform

Academic pipeline that ingests Our World in Data (OWID) COVID-19 records, processes them with **Apache Spark**, optionally replays them through **Kafka + Spark Structured Streaming**, stores analytics in **PostgreSQL**, and visualizes results in **Grafana**.

Historical statistics come from OWID. Synthetic Kafka records are labelled `source_type=synthetic` and are excluded from historical Grafana views by default.

## Architecture

```
OWID CSV ──► ingestion (Python) ──► PostgreSQL
                │
                └──► PySpark batch ──► Parquet + PostgreSQL
                                              ▲
Kafka topic covid-raw-records ──► Spark Structured Streaming ──┘
        ▲
   producer (historical_replay | synthetic)

Grafana (read-only SQL)     FastAPI (health, summary, replay control)
```

Data source types:

| `source_type`         | Meaning                                      | Default in Grafana |
|-----------------------|----------------------------------------------|--------------------|
| `historical`          | Cleaned OWID load                            | yes                |
| `historical_replay`   | Same historical rows republished via Kafka   | no                 |
| `synthetic`           | Demo generator, not real epidemiology        | no                 |

Duplicate rule: last `(iso_code, date, source_type)` row in the file wins.

Revision rule: if cumulative cases/deaths fall, `revision_flag` is set, original reported daily values are stored, and that day is excluded from rolling averages so a restatement is not turned into fake daily incidence.

Regional increase signals compare the latest 7-day average of daily cases with the previous 7-day average. They are **not** outbreak predictions.

## Prerequisites

- Docker Desktop / Docker Compose
- Python 3.11 (`py -3.11` on Windows)
- Java 17+ (needed only if you run PySpark on the host)

Python 3.13 is installed on some machines; use **3.11** for this project.

## Environment

```bash
copy .env.example .env
py -3.11 -m pip install -r requirements.txt
```

Default credentials are for local development only.

## Start services

Lightweight stack (Postgres, Kafka, Grafana, API) — recommended for a laptop:

```bash
docker compose up -d postgres kafka zookeeper grafana
py -3.11 scripts/apply_schema.py
```

Optional API container:

```bash
docker compose up -d --build api
```

Optional Spark cluster UI (extra memory):

```bash
docker compose --profile cluster up -d spark-master spark-worker
```

Wait until `docker compose ps` shows healthy/up. Kafka advertised listeners:

- inside Compose: `kafka:9092`
- on the host: `localhost:29092`

## Ingest historical data

Uses the cached `data/owid-covid-data.csv` if present (~98 MB). If the download fails, the offline fixture `data/fixtures/covid_small.csv` is used.

```bash
py -3.11 -m ingestion.download_data
py -3.11 -m ingestion.ingest_to_postgres
```

Offline fixture only:

```bash
py -3.11 -m ingestion.ingest_to_postgres --fixture
```

Verify:

```bash
py -3.11 scripts/verify_db.py
```

## Spark batch processing

```bash
py -3.11 -m spark.batch.process_historical --fixture
py -3.11 -m spark.batch.process_historical
```

Or in Docker (fixture, local Spark master, no cluster):

```bash
docker compose --profile batch run --rm spark-batch
```

Writes:

- `data/parquet/cleaned` and `data/parquet/aggregates`
- upserts `raw_covid_data`, `cleaned_covid_data`, `regional_aggregates`, `regional_trend_signals`

## Kafka replay and Spark streaming

Create/use topic `covid-raw-records` (auto-created).

Producer (host):

```bash
py -3.11 -m producer.kafka_producer --mode historical --limit 200 --speed 0.01
py -3.11 -m producer.kafka_producer --mode synthetic --limit 20 --speed 0.2
```

Streaming consumer (downloads the Spark Kafka connector on first run):

```bash
py -3.11 -m spark.streaming.process_streaming
```

Or:

```bash
docker compose --profile streaming up spark-streaming
```

API controls (does not replace Grafana):

- `POST /api/pipeline/replay/start` `{"mode":"historical","limit":100,"speed":0.01}`
- `POST /api/pipeline/replay/stop`

Delivery is **at-least-once**: Kafka offsets are checkpointed and PostgreSQL upserts are idempotent. Exactly-once is not claimed.

## Grafana

- URL: http://localhost:3000
- Login: `admin` / `admin`
- Dashboard: **COVID-19 Analytics Platform**
- Datasource: PostgreSQL as `grafana_reader` (SELECT only)

Variables: **Data source type** (`historical` default) and **Country**. Time range defaults to 2020–2024.

## API

http://localhost:8000/docs

- `GET /health`
- `GET /api/summary`
- `GET /api/trends`
- `GET /api/regions`
- `GET /api/pipeline/status`

## Tests

```bash
py -3.11 -m pytest tests -v
```

Integration tests need PostgreSQL. Spark tests need a local Spark session.

## Reset demo data

Removes replay/synthetic/E2E rows; keeps historical OWID data:

```bash
py -3.11 scripts/reset_demo.py
```

Full database reset (destroys the volume):

```bash
docker compose down -v
docker compose up -d postgres kafka zookeeper grafana
py -3.11 scripts/apply_schema.py
```

## College demonstration sequence

1. `docker compose up -d postgres kafka zookeeper grafana`
2. `py -3.11 scripts/apply_schema.py`
3. `py -3.11 -m ingestion.ingest_to_postgres --fixture` (or without `--fixture` if the OWID CSV is present)
4. `py -3.11 -m spark.batch.process_historical --fixture`
5. `py -3.11 scripts/verify_db.py`
6. Open Grafana and show daily cases, 7-day averages, CFR, and regional increase signals.
7. `py -3.11 -m spark.streaming.process_streaming` in one terminal.
8. `py -3.11 -m producer.kafka_producer --mode historical --limit 50 --speed 0.01`
9. Switch Grafana `source_type` to `historical_replay`, then `--mode synthetic` and `synthetic`.
10. `py -3.11 -m pytest tests -v`

## Troubleshooting

| Symptom | What to try |
|---------|-------------|
| `python` is 3.13 / store alias | Use `py -3.11` |
| Postgres relation does not exist | `py -3.11 scripts/apply_schema.py` (init SQL only runs on an empty volume) |
| Grafana datasource fails | Confirm `grafana_reader` exists; re-run apply_schema; user is `grafana_reader` not `covid_user` |
| Kafka connection refused | Wait for broker health; host bootstrap is `localhost:29092` |
| Spark cannot reach Kafka from Docker | Set `KAFKA_BROKER=kafka:9092` inside Compose |
| Spark OOM | Run `--fixture` or `docker compose --profile batch` with 2g driver; avoid `--profile cluster` on 8 GB RAM |
| Duplicate historical rows | Upserts key on `(iso_code, date, source_type)`; re-running ingest is safe |
| Negative daily cases | Revisions are flagged; originals kept; rolling averages skip that day |
| Dashboard empty | Widen time range to 2020–2024; set source type to `historical` |

## Project layout

```
ingestion/   download, validate, clean, upsert
spark/batch  PySpark historical job
spark/streaming  Structured Streaming consumer
producer/    Kafka historical replay and synthetic demo
analytics/   rolling averages, CFR, regional increase signals
api/         FastAPI
grafana/     provisioning + dashboard JSON
sql/         init + migrate
tests/       unit, Spark, and PostgreSQL integration tests
scripts/     schema, verify, reset
```
