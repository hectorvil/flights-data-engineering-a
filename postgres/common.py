"""Utilidades compartidas para PostgreSQL del proyecto flights.

Este módulo centraliza:
- lectura de credenciales desde Secrets Manager o argumentos manuales
- construcción del engine de SQLAlchemy
- normalización de valores nulos para bulk inserts
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DbCredentials:
    """Credenciales mínimas necesarias para conectarse a PostgreSQL."""

    username: str
    password: str
    dbname: str
    port: int = 5432


def _all_manual_fields_present(
    username: str | None,
    password: str | None,
    dbname: str | None,
    port: int | None,
) -> bool:
    return all(value is not None for value in (username, password, dbname, port))


def get_db_credentials(
    *,
    secret_id: str | None,
    region: str,
    username: str | None = None,
    password: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> DbCredentials:
    """Resolver credenciales desde Secrets Manager o valores manuales.

    Prioridad:
    1. Si se proporcionan manualmente todos los campos, se usan esos.
    2. Si no, intenta leer desde Secrets Manager.
    3. Como último recurso, revisa variables de entorno.
    """

    if _all_manual_fields_present(username, password, dbname, port):
        return DbCredentials(
            username=str(username),
            password=str(password),
            dbname=str(dbname),
            port=int(port),
        )

    env_username = os.getenv("FLIGHTS_DB_USER")
    env_password = os.getenv("FLIGHTS_DB_PASSWORD")
    env_dbname = os.getenv("FLIGHTS_DB_NAME")
    env_port = os.getenv("FLIGHTS_DB_PORT")

    if _all_manual_fields_present(env_username, env_password, env_dbname, int(env_port) if env_port else None):
        return DbCredentials(
            username=str(env_username),
            password=str(env_password),
            dbname=str(env_dbname),
            port=int(env_port),
        )

    if not secret_id:
        raise ValueError(
            "No fue posible resolver credenciales. Proporciona --secret-id o "
            "los cuatro parámetros manuales (--db-user, --db-password, --db-name, --db-port)."
        )

    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_id)
    payload = json.loads(secret["SecretString"])

    return DbCredentials(
        username=str(payload["username"]),
        password=str(payload["password"]),
        dbname=str(payload["dbname"]),
        port=int(payload["port"]),
    )


def build_engine(host: str, creds: DbCredentials) -> Engine:
    """Construir engine SQLAlchemy para PostgreSQL."""

    return create_engine(
        f"postgresql+psycopg2://{creds.username}:{creds.password}"
        f"@{host}:{creds.port}/{creds.dbname}"
    )


def normalize_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convertir NaN/NaT a None para bulk insert seguro en PostgreSQL."""

    return [
        {key: (None if pd.isnull(value) else value) for key, value in row.items()}
        for row in df.to_dict(orient="records")
    ]
