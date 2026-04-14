"""Cargar CSVs de flights a PostgreSQL usando bulk insert de SQLAlchemy 2.0."""

import argparse
import json
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from postgres.models import Airline, Airport, Flight


def parse_args():
    parser = argparse.ArgumentParser(description="Cargar datos de flights a PostgreSQL.")
    parser.add_argument("--host", required=True, help="Endpoint primario RDS")
    parser.add_argument("--data-dir", required=True, help="Directorio con los CSVs")
    parser.add_argument(
        "--secret-id",
        default="itam/rds/northwind/credentials",
        help="Secret de Secrets Manager",
    )
    parser.add_argument("--region", default="us-east-1", help="Región AWS")
    return parser.parse_args()


def get_db_creds(secret_id: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_id)
    return json.loads(secret["SecretString"])


def build_engine(host: str, creds: dict):
    return create_engine(
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{host}:{creds['port']}/{creds['dbname']}"
    )


def load_csv(session, model, path, rename_map=None, usecols=None, nrows=None):
    df = pd.read_csv(path, usecols=usecols, nrows=nrows)

    if rename_map:
        df = df.rename(columns=rename_map)

    records = [
        {k: None if pd.isnull(v) else v for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]

    session.execute(insert(model), records)
    print(f"✓ {model.__tablename__}: {len(records):,} filas cargadas")


def main():
    args = parse_args()
    creds = get_db_creds(args.secret_id, args.region)
    engine = build_engine(args.host, creds)
    data_dir = Path(args.data_dir)

    with Session(engine) as session:
        # 1. airlines
        load_csv(
            session,
            Airline,
            data_dir / "airlines.csv",
            rename_map={
                "IATA_CODE": "iata_code",
                "AIRLINE": "airline",
            },
        )

        # 2. airports
        load_csv(
            session,
            Airport,
            data_dir / "airports.csv",
            rename_map={
                "IATA_CODE": "iata_code",
                "AIRPORT": "airport",
                "CITY": "city",
                "STATE": "state",
            },
        )

        # 3. flights (solo 500k)
        load_csv(
            session,
            Flight,
            data_dir / "flights.csv",
            usecols=[
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
            ],
            rename_map={
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
            },
            nrows=500_000,
        )

        session.commit()
        print("✓ Bootstrap completo")


if __name__ == "__main__":
    main()