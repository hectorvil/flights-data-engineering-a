"""Silver ETL del Flights Dataset.

Uso:
    python etl/silver.py --bucket <tu-bucket>

Qué hace:
- Lee Bronze desde S3
- Estandariza tipos
- Calcula las 3 agregaciones requeridas:
  - flights_daily
  - flights_monthly
  - flights_by_airport
- Escribe Parquet + Snappy en Glue/S3
"""

from __future__ import annotations

import argparse
import logging
import sys
from functools import reduce

import awswrangler as wr
import pandas as pd

LOGGER = logging.getLogger(__name__)

BRONZE_DATABASE = "flights_bronze"
SILVER_DATABASE = "flights_silver"
DELAY_COMPONENTS = [
    "air_system_delay",
    "airline_delay",
    "weather_delay",
    "late_aircraft_delay",
    "security_delay",
]


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Bucket donde viven Bronze y Silver.")
    return parser.parse_args()


def bronze_path(bucket: str, table_name: str) -> str:
    return f"s3://{bucket}/flights/bronze/{table_name}/"


def silver_path(bucket: str, table_name: str) -> str:
    return f"s3://{bucket}/flights/silver/{table_name}/"


def list_bronze_files(bucket: str, table_name: str) -> list[str]:
    files = wr.s3.list_objects(path=bronze_path(bucket, table_name), suffix=".parquet")
    assert files, f"No se encontraron archivos parquet en Bronze/{table_name}"
    return sorted(files)


def standardize_flights_chunk(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "year",
        "month",
        "day",
        "airline",
        "origin_airport",
        "destination_airport",
        "departure_delay",
        "arrival_delay",
        "cancelled",
        "cancellation_reason",
        "distance",
        "air_system_delay",
        "airline_delay",
        "weather_delay",
        "late_aircraft_delay",
        "security_delay",
    ]
    missing = set(expected_columns).difference(df.columns)
    assert not missing, f"El chunk de Bronze/flights no trae columnas requeridas: {sorted(missing)}"

    for column in ["year", "month", "day", "cancelled"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    numeric_cols = [
        "departure_delay",
        "arrival_delay",
        "distance",
        "air_system_delay",
        "airline_delay",
        "weather_delay",
        "late_aircraft_delay",
        "security_delay",
    ]
    for column in numeric_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    assert not df.empty, "Chunk de flights vacío"
    assert df["year"].notna().all(), "year contiene nulos inesperados"
    assert df["month"].notna().all(), "month contiene nulos inesperados"
    assert df["day"].notna().all(), "day contiene nulos inesperados"

    return df


def build_daily_partial(df: pd.DataFrame) -> pd.DataFrame:
    active = df["cancelled"].fillna(0).eq(0)
    dep_delayed = df["departure_delay"].gt(0).fillna(False)
    dep_valid = active & df["departure_delay"].notna()
    arr_valid = active & df["arrival_delay"].notna()

    partial = (
        df.assign(
            total_flights=1,
            total_delayed=dep_delayed.astype(int),
            total_cancelled=df["cancelled"].fillna(0).astype(int),
            dep_delay_sum=df["departure_delay"].where(dep_valid, 0.0),
            dep_delay_count=dep_valid.astype(int),
            arr_delay_sum=df["arrival_delay"].where(arr_valid, 0.0),
            arr_delay_count=arr_valid.astype(int),
        )
        .groupby(["year", "month", "day"], as_index=False)[
            [
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "dep_delay_sum",
                "dep_delay_count",
                "arr_delay_sum",
                "arr_delay_count",
            ]
        ]
        .sum()
    )
    return partial


def finalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    final = (
        df.groupby(["year", "month", "day"], as_index=False)[
            [
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "dep_delay_sum",
                "dep_delay_count",
                "arr_delay_sum",
                "arr_delay_count",
            ]
        ]
        .sum()
        .assign(
            avg_departure_delay=lambda x: x["dep_delay_sum"] / x["dep_delay_count"].replace({0: pd.NA}),
            avg_arrival_delay=lambda x: x["arr_delay_sum"] / x["arr_delay_count"].replace({0: pd.NA}),
        )
        .drop(columns=["dep_delay_sum", "dep_delay_count", "arr_delay_sum", "arr_delay_count"])
        .sort_values(["month", "day"])
        .reset_index(drop=True)
    )

    assert not final.empty, "flights_daily quedó vacía"
    return final


def build_monthly_partial(df: pd.DataFrame) -> pd.DataFrame:
    active = df["cancelled"].fillna(0).eq(0)
    dep_delayed = df["departure_delay"].gt(0).fillna(False)
    arr_valid = active & df["arrival_delay"].notna()
    on_time = arr_valid & df["arrival_delay"].le(15)

    partial = (
        df.assign(
            total_flights=1,
            total_delayed=dep_delayed.astype(int),
            total_cancelled=df["cancelled"].fillna(0).astype(int),
            arr_delay_sum=df["arrival_delay"].where(arr_valid, 0.0),
            arr_delay_count=arr_valid.astype(int),
            on_time_count=on_time.astype(int),
        )
        .groupby(["month", "airline"], as_index=False)[
            [
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "arr_delay_sum",
                "arr_delay_count",
                "on_time_count",
            ]
        ]
        .sum()
    )
    return partial


def finalize_monthly(df: pd.DataFrame) -> pd.DataFrame:
    final = (
        df.groupby(["month", "airline"], as_index=False)[
            [
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "arr_delay_sum",
                "arr_delay_count",
                "on_time_count",
            ]
        ]
        .sum()
        .assign(
            avg_arrival_delay=lambda x: x["arr_delay_sum"] / x["arr_delay_count"].replace({0: pd.NA}),
            on_time_pct=lambda x: 100 * x["on_time_count"] / x["arr_delay_count"].replace({0: pd.NA}),
        )
        .drop(columns=["arr_delay_sum", "arr_delay_count", "on_time_count"])
        .sort_values(["month", "airline"])
        .reset_index(drop=True)
    )

    assert not final.empty, "flights_monthly quedó vacía"
    return final


def build_airport_partial(df: pd.DataFrame) -> pd.DataFrame:
    active = df["cancelled"].fillna(0).eq(0)
    dep_delayed = df["departure_delay"].gt(0).fillna(False)
    dep_valid = active & df["departure_delay"].notna()

    causal_delay_total = df[DELAY_COMPONENTS].fillna(0).sum(axis=1)

    partial = (
        df.assign(
            total_departures=1,
            total_delayed=dep_delayed.astype(int),
            total_cancelled=df["cancelled"].fillna(0).astype(int),
            dep_delay_sum=df["departure_delay"].where(dep_valid, 0.0),
            dep_delay_count=dep_valid.astype(int),
            weather_delay_sum=df["weather_delay"].fillna(0.0),
            total_causal_delay_sum=causal_delay_total,
        )
        .groupby(["origin_airport"], as_index=False)[
            [
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "dep_delay_sum",
                "dep_delay_count",
                "weather_delay_sum",
                "total_causal_delay_sum",
            ]
        ]
        .sum()
    )
    return partial


def finalize_airport(df: pd.DataFrame) -> pd.DataFrame:
    final = (
        df.groupby(["origin_airport"], as_index=False)[
            [
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "dep_delay_sum",
                "dep_delay_count",
                "weather_delay_sum",
                "total_causal_delay_sum",
            ]
        ]
        .sum()
        .assign(
            avg_departure_delay=lambda x: x["dep_delay_sum"] / x["dep_delay_count"].replace({0: pd.NA}),
            pct_weather_delay=lambda x: 100
            * x["weather_delay_sum"]
            / x["total_causal_delay_sum"].replace({0: pd.NA}),
        )
        .drop(columns=["dep_delay_sum", "dep_delay_count", "weather_delay_sum", "total_causal_delay_sum"])
        .sort_values(["total_departures", "origin_airport"], ascending=[False, True])
        .reset_index(drop=True)
    )

    assert not final.empty, "flights_by_airport quedó vacía"
    return final


def combine_partials(partials: list[pd.DataFrame]) -> pd.DataFrame:
    assert partials, "No se generaron parciales para combinar"
    return pd.concat(partials, ignore_index=True)


def write_silver(df: pd.DataFrame, *, bucket: str, table_name: str, partition_cols: list[str] | None = None) -> None:
    kwargs = dict(
        df=df,
        path=silver_path(bucket, table_name),
        dataset=True,
        database=SILVER_DATABASE,
        table=table_name,
        compression="snappy",
        sanitize_columns=True,
    )
    if partition_cols:
        kwargs["partition_cols"] = partition_cols
        kwargs["mode"] = "overwrite_partitions"
    else:
        kwargs["mode"] = "overwrite"

    wr.s3.to_parquet(**kwargs)
    LOGGER.info("Silver/%s escrita en %s", table_name, silver_path(bucket, table_name))


def main() -> None:
    args = parse_args()

    try:
        wr.catalog.create_database(name=SILVER_DATABASE, exist_ok=True)
        LOGGER.info("Base Glue '%s' lista.", SILVER_DATABASE)

        bronze_files = list_bronze_files(args.bucket, "flights")
        LOGGER.info("Se encontraron %s archivos parquet en Bronze/flights.", len(bronze_files))

        daily_partials: list[pd.DataFrame] = []
        monthly_partials: list[pd.DataFrame] = []
        airport_partials: list[pd.DataFrame] = []

        for idx, file_path in enumerate(bronze_files, start=1):
            chunk = wr.s3.read_parquet(path=file_path)
            chunk = standardize_flights_chunk(chunk)

            daily_partials.append(build_daily_partial(chunk))
            monthly_partials.append(build_monthly_partial(chunk))
            airport_partials.append(build_airport_partial(chunk))

            LOGGER.info("Chunk parquet %s/%s procesado: %s filas", idx, len(bronze_files), len(chunk))

        flights_daily = finalize_daily(combine_partials(daily_partials))
        flights_monthly = finalize_monthly(combine_partials(monthly_partials))
        flights_by_airport = finalize_airport(combine_partials(airport_partials))

        write_silver(flights_daily, bucket=args.bucket, table_name="flights_daily", partition_cols=["month"])
        write_silver(flights_monthly, bucket=args.bucket, table_name="flights_monthly")
        write_silver(flights_by_airport, bucket=args.bucket, table_name="flights_by_airport")

        LOGGER.info(
            "Silver completada. flights_daily=%s flights_monthly=%s flights_by_airport=%s",
            len(flights_daily),
            len(flights_monthly),
            len(flights_by_airport),
        )
    except Exception:  # pragma: no cover - fail fast para CLI
        LOGGER.exception("Falló la construcción de la capa Silver.")
        sys.exit(1)


if __name__ == "__main__":
    setup_logging()
    main()
