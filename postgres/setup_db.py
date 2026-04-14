"""Crear schema PostgreSQL del dataset de vuelos."""

import argparse
import json

import boto3
from sqlalchemy import create_engine

from postgres.models import Base


def parse_args():
    parser = argparse.ArgumentParser(description="Crear tablas PostgreSQL para flights.")
    parser.add_argument("--host", required=True, help="Endpoint primario RDS")
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


def main():
    args = parse_args()
    creds = get_db_creds(args.secret_id, args.region)

    engine = create_engine(
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{args.host}:{creds['port']}/{creds['dbname']}"
    )

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    print("✓ Schema creado en PostgreSQL")


if __name__ == "__main__":
    main()