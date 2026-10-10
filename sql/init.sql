-- Fresh-database bootstrap. Also applied by scripts/apply_schema.py on existing volumes.

CREATE TABLE IF NOT EXISTS raw_covid_data (
    iso_code VARCHAR(16) NOT NULL,
    continent VARCHAR(50),
    location VARCHAR(120),
    date DATE NOT NULL,
    total_cases DOUBLE PRECISION,
    new_cases DOUBLE PRECISION,
    total_deaths DOUBLE PRECISION,
    new_deaths DOUBLE PRECISION,
    original_new_cases DOUBLE PRECISION,
    original_new_deaths DOUBLE PRECISION,
    daily_cases_derived DOUBLE PRECISION,
    daily_deaths_derived DOUBLE PRECISION,
    revision_flag BOOLEAN DEFAULT FALSE,
    source_type VARCHAR(32) NOT NULL DEFAULT 'historical',
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    PRIMARY KEY (iso_code, date, source_type)
);

CREATE TABLE IF NOT EXISTS cleaned_covid_data (
    iso_code VARCHAR(16) NOT NULL,
    continent VARCHAR(50),
    location VARCHAR(120),
    date DATE NOT NULL,
    total_cases DOUBLE PRECISION,
    total_deaths DOUBLE PRECISION,
    reported_new_cases DOUBLE PRECISION,
    reported_new_deaths DOUBLE PRECISION,
    daily_cases DOUBLE PRECISION,
    daily_deaths DOUBLE PRECISION,
    revision_flag BOOLEAN DEFAULT FALSE,
    source_type VARCHAR(32) NOT NULL DEFAULT 'historical',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (iso_code, date, source_type)
);

CREATE TABLE IF NOT EXISTS regional_aggregates (
    iso_code VARCHAR(16) NOT NULL,
    continent VARCHAR(50),
    location VARCHAR(120),
    date DATE NOT NULL,
    total_cases DOUBLE PRECISION,
    total_deaths DOUBLE PRECISION,
    new_cases DOUBLE PRECISION,
    new_deaths DOUBLE PRECISION,
    rolling_7d_cases DOUBLE PRECISION,
    rolling_7d_deaths DOUBLE PRECISION,
    case_fatality_ratio DOUBLE PRECISION,
    source_type VARCHAR(32) NOT NULL DEFAULT 'historical',
    PRIMARY KEY (iso_code, date, source_type)
);

CREATE TABLE IF NOT EXISTS regional_trend_signals (
    iso_code VARCHAR(16) NOT NULL,
    location VARCHAR(120),
    date DATE NOT NULL,
    recent_7d_avg DOUBLE PRECISION,
    previous_7d_avg DOUBLE PRECISION,
    percentage_change DOUBLE PRECISION,
    is_increasing BOOLEAN,
    observation_count INTEGER,
    threshold_pct DOUBLE PRECISION,
    explanation TEXT,
    source_type VARCHAR(32) NOT NULL DEFAULT 'historical',
    PRIMARY KEY (iso_code, date, source_type)
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(80),
    records_processed INT,
    latest_date DATE,
    process_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS pipeline_status (
    process_name VARCHAR(80) PRIMARY KEY,
    status VARCHAR(32),
    records_in BIGINT DEFAULT 0,
    records_out BIGINT DEFAULT 0,
    records_quarantined BIGINT DEFAULT 0,
    last_error TEXT,
    extra JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_control (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    replay_requested BOOLEAN DEFAULT FALSE,
    stop_requested BOOLEAN DEFAULT FALSE,
    replay_mode VARCHAR(20) DEFAULT 'historical',
    replay_limit INTEGER,
    replay_speed DOUBLE PRECISION DEFAULT 0.05,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO pipeline_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS quarantined_records (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    error_reason TEXT,
    source_type VARCHAR(32),
    quarantine_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_location_date ON raw_covid_data (location, date);
CREATE INDEX IF NOT EXISTS idx_raw_source ON raw_covid_data (source_type);
CREATE INDEX IF NOT EXISTS idx_agg_location_date ON regional_aggregates (location, date);
CREATE INDEX IF NOT EXISTS idx_agg_source_date ON regional_aggregates (source_type, date);
CREATE INDEX IF NOT EXISTS idx_trend_increasing ON regional_trend_signals (is_increasing, percentage_change DESC);
CREATE INDEX IF NOT EXISTS idx_cleaned_source ON cleaned_covid_data (source_type, date);

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_reader_pass';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE covid_db TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
