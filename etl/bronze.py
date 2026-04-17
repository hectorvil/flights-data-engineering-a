"""Bronze ETL del Flights Dataset.

Uso:
    python etl/bronze.py --bucket <tu-bucket> --data-dir data/

Qué hace:
- Lee los CSVs locales
- Crea la base `flights_bronze` en Glue
- Sube `airlines`, `airports` y `flights` a S3/Glue
- Para `flights`, usa chunking para evitar problemas de memoria

Notas:
- Bronze preserva los datos de la fuente sin transformar valores.
- Solo se estandarizan nombres de columnas para compatibilidad con Glue/Athena.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import awswrangler as wr
import pandas as pd

LOGGER = logging.getLogger(__name__)

BRONZE_DATABASE = "flights_bronze"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Bucket destino en S3.")
    parser.add_argument("--data-dir", required=True, help="Directorio local con los CSVs.")
    parser.add_argument(
        "--flights-chunksize",
        type=int,
        default=250_000,
        help="Tamaño de chunk para flights.csv.",
    )
    return parser.parse_args()


def s3_path(bucket: str, table_name: str) -> str:
    return f"s3://{bucket}/flights/bronze/{table_name}/"


def validate_local_file(path: Path) -> None:
    assert path.exists() and path.is_file(), f"No existe el archivo requerido: {path}"


def validate_df(df: pd.DataFrame, key_columns: list[str], table_name: str) -> None:
    assert not df.empty, f"{table_name} llegó vacío"
    for column in key_columns:
        assert column in df.columns, f"{table_name} no contiene la columna requerida '{column}'"
        assert df[column].notna().all(), f"{table_name}.{column} contiene nulos inesperados"


def write_table(
    df: pd.DataFrame,
    *,
    bucket: str,
    table_name: str,
    mode: str,
) -> None:
    wr.s3.to_parquet(
        df=df,
        path=s3_path(bucket, table_name),
        dataset=True,
        database=BRONZE_DATABASE,
        table=table_name,
        mode=mode,
        sanitize_columns=True,
    )


def load_small_csv(path: Path, *, bucket: str, table_name: str, key_columns: list[str]) -> int:
    df = pd.read_csv(path)
    validate_df(df, key_columns=key_columns, table_name=table_name)

    write_table(df, bucket=bucket, table_name=table_name, mode="overwrite")
    LOGGER.info(
        "Bronze/%s cargada con %s filas en %s",
        table_name,
        len(df),
        s3_path(bucket, table_name),
    )
    return len(df)


def load_flights_in_chunks(path: Path, *, bucket: str, chunksize: int) -> int:
    total_rows = 0

    for chunk_number, chunk in enumerate(
        pd.read_csv(path, chunksize=chunksize, low_memory=False),
        start=1,
    ):
        validate_df(
            chunk,
            key_columns=["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"],
            table_name="flights",
        )

        mode = "overwrite" if chunk_number == 1 else "append"
        write_table(chunk, bucket=bucket, table_name="flights", mode=mode)

        total_rows += len(chunk)
        LOGGER.info(
            "Bronze/flights chunk=%s filas=%s total_acumulado=%s destino=%s",
            chunk_number,
            len(chunk),
            total_rows,
            s3_path(bucket, "flights"),
        )

    return total_rows


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    airlines_path = data_dir / "airlines.csv"
    airports_path = data_dir / "airports.csv"
    flights_path = data_dir / "flights.csv"

    validate_local_file(airlines_path)
    validate_local_file(airports_path)
    validate_local_file(flights_path)

    try:
        wr.catalog.create_database(name=BRONZE_DATABASE, exist_ok=True)
        LOGGER.info("Base Glue '%s' lista.", BRONZE_DATABASE)

        airlines_rows = load_small_csv(
            airlines_path,
            bucket=args.bucket,
            table_name="airlines",
            key_columns=["IATA_CODE", "AIRLINE"],
        )

        airports_rows = load_small_csv(
            airports_path,
            bucket=args.bucket,
            table_name="airports",
            key_columns=["IATA_CODE", "AIRPORT", "CITY", "STATE"],
        )

        flights_rows = load_flights_in_chunks(
            flights_path,
            bucket=args.bucket,
            chunksize=args.flights_chunksize,
        )

        LOGGER.info(
            "Bronze completada. airlines=%s airports=%s flights=%s",
            airlines_rows,
            airports_rows,
            flights_rows,
        )
    except Exception:  # pragma: no cover - fail fast para CLI
        LOGGER.exception("Falló la construcción de la capa Bronze.")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
