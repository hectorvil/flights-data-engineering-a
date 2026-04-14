"""Gold ETL del Flights Dataset.

Uso:
    python etl/gold.py --bucket <tu-bucket>

Qué hace:
- crea la base flights_gold
- elimina la tabla vuelos_analitica si existe
- ejecuta un CTAS en Athena
- valida que la tabla responda a un SELECT LIMIT 5
"""

from __future__ import annotations

import argparse
import logging
import sys

import awswrangler as wr

LOGGER = logging.getLogger(__name__)

GOLD_DATABASE = "flights_gold"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Bucket destino del data lake.")
    parser.add_argument(
        "--athena-output",
        default=None,
        help="Ruta de resultados de Athena. Si no se especifica, usa s3://<bucket>/athena-results/.",
    )
    return parser.parse_args()


def build_ctas(bucket: str) -> str:
    return f"""
    CREATE TABLE flights_gold.vuelos_analitica
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = 's3://{bucket}/flights/gold/vuelos_analitica/'
    ) AS
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport AS origin_airport_name,
        ap_orig.city AS origin_city,
        ap_orig.state AS origin_state,
        f.destination_airport,
        ap_dest.airport AS destination_airport_name,
        al.airline AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
    """.strip()


def main() -> None:
    args = parse_args()
    athena_output = args.athena_output or f"s3://{args.bucket}/athena-results/"

    try:
        wr.catalog.create_database(name=GOLD_DATABASE, exist_ok=True)
        wr.catalog.delete_table_if_exists(database=GOLD_DATABASE, table="vuelos_analitica")
        LOGGER.info("Base y tabla Gold preparadas.")

        ctas_query = build_ctas(args.bucket)

        wr.athena.read_sql_query(
            sql=ctas_query,
            database=GOLD_DATABASE,
            ctas_approach=False,
            s3_output=athena_output,
        )
        LOGGER.info("CTAS Gold ejecutado correctamente.")

        sample = wr.athena.read_sql_query(
            sql="SELECT * FROM flights_gold.vuelos_analitica LIMIT 5",
            database=GOLD_DATABASE,
            ctas_approach=False,
            s3_output=athena_output,
        )
        assert not sample.empty, "La validación de Gold devolvió 0 filas"
        LOGGER.info("Validación Gold OK. SELECT LIMIT 5 devolvió %s filas.", len(sample))
    except Exception:  # pragma: no cover - fail fast para CLI
        LOGGER.exception("Falló la construcción de la capa Gold.")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
