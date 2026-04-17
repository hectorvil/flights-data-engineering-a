"""Crear el schema PostgreSQL del proyecto flights.

Uso típico con Secrets Manager:
    python postgres/setup_db.py --host <RdsEndpoint> --secret-id itam/rds/northwind/credentials

Uso alternativo con credenciales manuales:
    python postgres/setup_db.py --host <RdsEndpoint> --db-user itam --db-password *** --db-name flights --db-port 5432
"""

from __future__ import annotations

import argparse
import logging
import sys

from postgres.common import build_engine, get_db_credentials
from postgres.models import Base

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
    parser.add_argument("--region", default="us-east-1", help="Región AWS del secret.")
    parser.add_argument("--secret-id", default=None, help="Secret de Secrets Manager.")
    parser.add_argument("--db-user", default=None, help="Usuario PostgreSQL (opcional).")
    parser.add_argument("--db-password", default=None, help="Password PostgreSQL (opcional).")
    parser.add_argument("--db-name", default=None, help="Nombre de la base (opcional).")
    parser.add_argument("--db-port", type=int, default=None, help="Puerto PostgreSQL (opcional).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

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

        LOGGER.info("Eliminando tablas existentes (si existen).")
        Base.metadata.drop_all(engine)

        LOGGER.info("Creando tablas nuevas.")
        Base.metadata.create_all(engine)

        LOGGER.info("Schema PostgreSQL creado exitosamente.")
    except Exception:  # pragma: no cover - fail fast para CLI
        LOGGER.exception("No fue posible crear el schema PostgreSQL.")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
