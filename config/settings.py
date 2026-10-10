import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


@dataclass(frozen=True)
class Settings:
    postgres_user: str = _env("POSTGRES_USER", "covid_user")
    postgres_password: str = _env("POSTGRES_PASSWORD", "covid_password")
    postgres_db: str = _env("POSTGRES_DB", "covid_db")
    postgres_host: str = _env("POSTGRES_HOST", "localhost")
    postgres_port: str = _env("POSTGRES_PORT", "5432")
    db_url: str = _env(
        "DB_URL",
        "postgresql://covid_user:covid_password@localhost:5432/covid_db",
    )
    jdbc_url: str = _env("JDBC_URL", "jdbc:postgresql://localhost:5432/covid_db")
    db_user: str = _env("DB_USER", "covid_user")
    db_password: str = _env("DB_PASSWORD", "covid_password")
    kafka_broker: str = _env("KAFKA_BROKER", "localhost:29092")
    kafka_topic: str = _env("KAFKA_TOPIC", "covid-raw-records")
    trend_threshold_pct: float = float(_env("TREND_THRESHOLD_PCT", "10"))
    trend_min_obs: int = int(_env("TREND_MIN_OBS", "7"))
    replay_speed: float = float(_env("REPLAY_SPEED", "0.05"))
    replay_limit: int = int(_env("REPLAY_LIMIT", "500"))


def get_settings() -> Settings:
    return Settings()
