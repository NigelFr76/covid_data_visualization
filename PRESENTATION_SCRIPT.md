# COVID-19 Big Data Analytics Platform: Presentation Script

**Presenter:** Nigel Francy (PRN: 23070122148)  
**Subject:** Big Data Analytics (CA3) · Symbiosis Institute of Technology, Pune  
**Setting:** Live Screen-Share on the Grafana Dashboard (`http://localhost:3002/d/covid_main/covid-19-analytics-platform`)  
**Core Presentation Focus:** Distributed Big Data Architecture, Pipeline Engineering, and Tech Stack Integration.

---

## 🎯 Quick Presentation Overview (5–7 Minutes)

```
0:00 - 0:45 ─── Hook & Tech Stack Architecture Overview
0:45 - 2:00 ─── Data Engineering: PySpark Batch Engine & Columnar Parquet
2:00 - 3:15 ─── Live Dashboard Tour: Decoupled SQL & Analytical Cubes
3:15 - 4:15 ─── Streaming Architecture: Kafka Replay & Spark Structured Streaming
4:15 - 5:00 ─── Fault Tolerance: Idempotency, Quarantining & Grafana Role Decoupling
5:00+       ─── Conclusion & Technical Q&A
```

---

## 🎬 Minute-by-Minute Presentation Script

### [0:00 - 0:45] Introduction & The Big Data Problem
*(Action: Have the Grafana Dashboard open in fullscreen. Keep your mouse resting near the header.)*

> **"Good morning, respected professors and evaluators.**  
> My name is **Nigel Francy**, and today I am presenting an end-to-end Big Data Analytics and Real-Time Surveillance Platform.
>
> While the dataset represents multi-year longitudinal COVID-19 records from Our World in Data comprising over **429,000 dense observations across 255 countries**, the core focus of this engineering project is **how we solve the classic Big Data challenges of Volume, Velocity, Variety, and Veracity using a modern distributed systems stack**.
>
> Rather than relying on fragile single-node Python scripts or slow full-table scans, this system integrates **Apache Spark** for distributed batch window computations, **Apache Kafka** for high-throughput event streaming, **Spark Structured Streaming** for micro-batch ingestion, **PostgreSQL** as a relational analytical warehouse, and **Grafana** for decoupled sub-second visual intelligence."

---

### [0:45 - 2:00] Deep-Dive: Distributed PySpark Batch Engine
*(Action: Hover your mouse over the "Seven-day rolling averages" and "Daily new cases" timeseries panels.)*

> **"Let's look at the data engineering under the hood:**
>
> **1. Handling Reporting Veracity in Spark:**  
> Epidemiological reporting is notorious for retroactive adjustments—authorities frequently drop cumulative figures to correct false positives. A naive difference in SQL or Pandas converts a cumulative drop into a massive fake negative daily spike.  
> In PySpark, we implemented a custom revision isolation protocol: using Window lag functions, whenever a negative derived count occurs, we flag it as `revision_flag = True`, preserve the raw reported count for compliance, and set the calculation denominator to `NULL` so downstream rolling averages are not corrupted.
>
> **2. Distributed Window Functions:**  
> To smooth out weekend reporting lags, PySpark applies distributed window partitions:  
> `avg(daily_cases) OVER (PARTITION BY iso_code ORDER BY date ROWS BETWEEN -6 AND 0)`  
> This executes in parallel across Spark worker threads and persists cleaned data into columnar **Apache Parquet partitions**, reducing storage by over 70% while enabling fast predicate pushdown."

---

### [2:00 - 3:15] Live Dashboard Tour: Decoupled SQL & Pre-Aggregated Cubes
*(Action: Move cursor across the stat cards, then down to the country comparison and regional increase tables.)*

> **"Now, let's explore the live dashboard in front of us:**
>
> **1. Sub-Second Query Execution:**  
> Notice how quickly this dashboard renders. We deliberately decoupled analytical ingestion from visualization. Grafana never queries heavy raw logs directly. Instead, Spark computes analytical cubes into PostgreSQL, and Grafana connects via a dedicated, read-only `grafana_reader` role.
>
> **2. The Stat Cards (Top Row):**  
> *(Point cursor at the four stat cards)*  
> Here you see our pre-aggregated totals: **775.8 Million confirmed cases**, **7.05 Million deaths**, tracking observations through **August 2024**, across **15,087 core country-date records**.
>
> **3. Smoothed Moving Trends vs. Raw Noise (Middle Panels):**  
> *(Hover over Panel 7: Seven-day rolling averages)*  
> In these middle time-series panels, you can see the raw daily spikes in the top graph contrasted with our smooth 7-day rolling window below. This demonstrates how the PySpark window aggregation filters high-frequency noise and highlights macroeconomic epidemic waves, like the massive global Omicron surge in early 2022.
>
> **4. Analytical Velocity Indicators (Table Panel 9):**  
> *(Point cursor to 'Regional increase signals')*  
> Down here, our analytics engine computes moving velocity ratios: comparing the latest 7-day mean against the preceding 7-day baseline:  
> `((recent_7d - previous_7d) / previous_7d) * 100%`  
> Notice how jurisdictions like India and the US in early transmission waves are automatically flagged with statistical surge explanations because their growth exceeded our configured 10% threshold."

---

### [3:15 - 4:15] Dual-Track Streaming: Apache Kafka & Spark Structured Streaming
*(Action: Click on the 'Data source type' dropdown at the top left of the dashboard. Point to 'historical', 'historical_replay', and 'synthetic'.)*

> **"One of the key engineering highlights of this project is our dual-track streaming architecture:**
>
> *(Point to the 'source_type' dropdown)*  
> In enterprise data engineering, you must prevent simulated or test records from polluting official historical data. We solved this by enforcing strict source-type partitioning:
>
> 1. `historical`: Represents the clean, immutable Our World in Data batch load.
> 2. `historical_replay`: Represents historical rows re-streamed through an **Apache Kafka topic (`covid-raw-records`)** to simulate live streaming ingestion.
> 3. `synthetic`: A live event generator simulating real-time sensor and clinic feeds.
>
> On the consumer side, **Spark Structured Streaming** ingests micro-batches from Kafka with **checkpointed offsets**. Delivery is **at-least-once**: if a network failure occurs, Spark resumes from the exact committed Kafka offset, while our database upsert logic handles deduplication."

---

### [4:15 - 5:00] Reliability, Idempotent Storage & Schema Quarantining
*(Action: Scroll down to Panel 13: 'Pipeline health, ingestion counts, and processing errors'.)*

> **"Finally, let's look at enterprise resilience and fault tolerance:**
>
> *(Hover over Panel 13 table)*  
> In Big Data pipelines, jobs will fail if bad records crash your workers. To prevent this, we built a **dead-letter quarantine engine**:
>
> - Every record without a valid ISO-3166 code or unparseable timestamp is isolated into a PostgreSQL `quarantined_records` table with the raw payload in a `JSONB` column and the failure reason.
> - Look at Panel 13: It live-audits our pipeline health: tracking input counts, output counts, and quarantined records in real time.
> - Furthermore, our PostgreSQL storage uses composite keys: `PRIMARY KEY (iso_code, date, source_type)`. All ingestion operates using `ON CONFLICT DO UPDATE`. This guarantees **complete idempotency**—meaning any batch or streaming micro-batch can be re-run indefinitely without generating duplicate rows."

---

### [5:00 - 5:30] Conclusion & Wrap-Up
*(Action: Scroll smoothly back to the top of the dashboard showing the global view.)*

> **"To summarize:**  
> This project demonstrates how **Apache Spark, Apache Kafka, PostgreSQL, FastAPI, and Grafana** work together to create an observable, fault-tolerant, high-performance big data pipeline. We have verified the pipeline with a 26-test automated suite covering data cleaning, PySpark transforms, API endpoints, and streaming serialization.
>
> Thank you, professors. I am now open to your technical questions."

---

## 💡 Bulletproof Answers for Technical Q&A (Viva)

### Q1: "Why did you use Apache Spark instead of Pandas or plain PostgreSQL queries?"
> **Answer:**  
> "Pandas is single-threaded and bounded by host RAM. On multi-gigabyte or streaming datasets, computing longitudinal rolling averages across 255 countries causes Out-Of-Memory (OOM) exceptions. Plain SQL window queries on raw logs also degrade dashboard responsiveness because every viewer forces full-table aggregations.  
> Apache Spark gives us distributed in-memory partitioning, resilient distributed datasets, lazy evaluation, and the ability to export optimized columnar Parquet partitions. Furthermore, PySpark uses the exact same DataFrame logic for both batch ETL and real-time Structured Streaming."

### Q2: "How do you guarantee data consistency and handle duplicate records in Kafka?"
> **Answer:**  
> "We implement **at-least-once delivery with idempotent writes**:  
> In Kafka and Spark Structured Streaming, offsets are persisted to a write-ahead log checkpoint directory (`data/checkpoints/streaming`). If a worker dies, Spark replays from the last checkpoint.  
> In PostgreSQL, our tables enforce a composite primary key: `(iso_code, date, source_type)`. All batch and streaming inserts use PostgreSQL's `ON CONFLICT (iso_code, date, source_type) DO UPDATE`. This makes all upserts mathematically idempotent—replaying a Kafka topic or re-running a batch job never creates duplicate observations."

### Q3: "What is your revision handling rule and why is it important?"
> **Answer:**  
> "In public health datasets, cumulative numbers are frequently revised downward to correct past overcounting. If you simply calculate daily cases as $C_t - C_{t-1}$, a downward revision of 5,000 cases looks like $-5,000$ new infections on that day.  
> Our Spark pipeline flags rows where derived daily cases are negative as `revision_flag = True`. We store the original reported value in `original_new_cases` for auditability, but set the analytical `daily_cases` to `NULL`. This ensures rolling 7-day averages skip the restatement day rather than displaying fake negative incidence."

### Q4: "How does Grafana query the database without impacting pipeline ingestion?"
> **Answer:**  
> "We established strict architectural decoupling:  
> 1. Ingestion jobs write using the privileged `covid_user` role.  
> 2. Grafana connects through a restricted, dedicated role called `grafana_reader`, which is granted `SELECT` privileges only.  
> 3. Grafana queries pre-aggregated dimensional tables (`regional_aggregates` and `regional_trend_signals`) indexed on `(source_type, date)` and `(location, date)`, ensuring queries execute in sub-millisecond index scans rather than full table scans."

### Q5: "What is the role of FastAPI in this project if Grafana is already there?"
> **Answer:**  
> "Grafana is purely a read-only visual analytics layer. It cannot trigger pipeline operations.  
> FastAPI acts as our programmable control plane and REST microservice:  
> - It exposes `/health` and `/api/pipeline/status` for orchestration monitoring.  
> - It exposes `/api/summary` and `/api/trends` for external microservices or mobile apps.  
> - Most importantly, it exposes POST endpoints (`/api/pipeline/replay/start` and `/stop`) to programmatically trigger asynchronous Kafka event producers."
