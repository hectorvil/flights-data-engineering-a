"""Cargar airlines, airports y flights en PostgreSQL usando bulk insert de SQLAlchemy 2.0.

Uso típico:
    python postgres/load_data.py --host <RdsEndpoint> --data-dir data/ --secret-id itam/rds/northwind/credentials
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import insert
from sqlalchemy.orm import Session

from postgres.common import build_engine, get_db_credentials, normalize_records
from postgres.models import Airline, Airport, Flight

LOGGER = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="Endpoint primario de RDS.")
    parser.add_argument("--data-dir", default="data", help="Directorio con airlines.csv, airports.csv y flights.csv.")
    parser.add_argument("--flights-nrows", type=int, default=500_000, help="Número máximo de vuelos a cargar.")
    parser.add_argument("--flights-chunksize", type=int, default=100_000, help="Tamaño de chunk para flights.")
    parser.add_argument("--region", default="us-east-1", help="Región AWS del secret.")
    parser.add_argument("--secret-id", default=None, help="Secret de Secrets Manager.")
    parser.add_argument("--db-user", default=None, help="Usuario PostgreSQL (opcional).")
    parser.add_argument("--db-password", default=None, help="Password PostgreSQL (opcional).")
    parser.add_argument("--db-name", default=None, help="Nombre de la base (opcional).")
    parser.add_argument("--db-port", type=int, default=None, help="Puerto PostgreSQL (opcional).")
    return parser.parse_args()


def validate_exists(path: Path) -> None:
    assert path.exists() and path.is_file(), f"No existe el archivo requerido: {path}"


def load_small_csv(session: Session, model, path: Path, rename_map: dict[str, str]) -> int:
    df = pd.read_csv(path)
    assert not df.empty, f"{path.name} llegó vacío"

    missing = set(rename_map).difference(df.columns)
    assert not missing, f"Faltan columnas esperadas en {path.name}: {sorted(missing)}"

    df = df[list(rename_map)].rename(columns=rename_map)

    if model is Airport:
        for col in ["latitude", "longitude"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    records = normalize_records(df)
    session.execute(insert(model), records)
    return len(records)


def _prepare_flights_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    keep_columns = [
        "YEAR",
        "MONTH",
        "DAY",
        "AIRLINE",
        "FLIGHT_NUMBER",
        "ORIGIN_AIRPORT",
        "DESTINATION_AIRPORT",
        "SCHEDULED_DEPARTURE",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
        "CANCELLED",
        "CANCELLATION_REASON",
        "DISTANCE",
        "AIR_SYSTEM_DELAY",
        "AIRLINE_DELAY",
        "WEATHER_DELAY",
        "LATE_AIRCRAFT_DELAY",
        "SECURITY_DELAY",
    ]

    missing = set(keep_columns).difference(chunk.columns)
    assert not missing, f"Faltan columnas esperadas en flights.csv: {sorted(missing)}"

    rename_map = {
        "YEAR": "year",
        "MONTH": "month",
        "DAY": "day",
        "AIRLINE": "airline",
        "FLIGHT_NUMBER": "flight_number",
        "ORIGIN_AIRPORT": "origin_airport",
        "DESTINATION_AIRPORT": "destination_airport",
        "SCHEDULED_DEPARTURE": "scheduled_departure",
        "DEPARTURE_DELAY": "departure_delay",
        "ARRIVAL_DELAY": "arrival_delay",
        "CANCELLED": "cancelled",
        "CANCELLATION_REASON": "cancellation_reason",
        "DISTANCE": "distance",
        "AIR_SYSTEM_DELAY": "air_system_delay",
        "AIRLINE_DELAY": "airline_delay",
        "WEATHER_DELAY": "weather_delay",
        "LATE_AIRCRAFT_DELAY": "late_aircraft_delay",
        "SECURITY_DELAY": "security_delay",
    }

    df = chunk[keep_columns].rename(columns=rename_map)

    integer_cols = [
        "year",
        "month",
        "day",
        "flight_number",
        "scheduled_departure",
        "cancelled",
    ]
    float_cols = [
        "departure_delay",
        "arrival_delay",
        "distance",
        "air_system_delay",
        "airline_delay",
        "weather_delay",
        "late_aircraft_delay",
        "security_delay",
    ]

    for col in integer_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    assert not df.empty, "El chunk de flights quedó vacío después del preprocesamiento"
    assert df["year"].notna().all(), "year no debe tener nulos"
    assert df["month"].notna().all(), "month no debe tener nulos"
    assert df["day"].notna().all(), "day no debe tener nulos"

    return df


def load_flights(session: Session, path: Path, nrows: int, chunksize: int) -> int:
    total_loaded = 0
    remaining = nrows

    for idx, chunk in enumerate(pd.read_csv(path, nrows=nrows, chunksize=chunksize, low_memory=False), start=1):
        df = _prepare_flights_chunk(chunk)

        records = normalize_records(df)
        session.execute(insert(Flight), records)
        session.commit()

        total_loaded += len(records)
        remaining -= len(records)

        LOGGER.info(
            "Chunk %s de flights insertado con %s filas. Total acumulado=%s",
            idx,
            len(records),
            total_loaded,
        )

        if remaining <= 0:
            break

    return total_loaded


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    airlines_path = data_dir / "airlines.csv"
    airports_path = data_dir / "airports.csv"
    flights_path = data_dir / "flights.csv"

    validate_exists(airlines_path)
    validate_exists(airports_path)
    validate_exists(flights_path)

    try:
        creds = get_db_credentials(
            secret_id=args.secret_id,
            region=args.region,
            username=args.db_user,
            password=args.db_password,
            dbname=args.db_name,
            port=args.db_port,
        )
        engine = build_engine(args.host, creds)

        with Session(engine) as session:
            airlines_loaded = load_small_csv(
                session,
                Airline,
                airlines_path,
                rename_map={
                    "IATA_CODE": "iata_code",
                    "AIRLINE": "airline",
                },
            )
            session.commit()
            LOGGER.info("airlines cargada con %s filas.", airlines_loaded)

            airports_loaded = load_small_csv(
                session,
                Airport,
                airports_path,
                rename_map={
                    "IATA_CODE": "iata_code",
                    "AIRPORT": "airport",
                    "CITY": "city",
                    "STATE": "state",
                    "LATITUDE": "latitude",
                    "LONGITUDE": "longitude",
                },
            )
            session.commit()
            LOGGER.info("airports cargada con %s filas.", airports_loaded)

            flights_loaded = load_flights(
                session=session,
                path=flights_path,
                nrows=args.flights_nrows,
                chunksize=args.flights_chunksize,
            )
            LOGGER.info("flights cargada con %s filas.", flights_loaded)

        LOGGER.info("Carga PostgreSQL completada exitosamente.")
    except Exception:  # pragma: no cover - fail fast para CLI
        LOGGER.exception("No fue posible cargar los datos en PostgreSQL.")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
