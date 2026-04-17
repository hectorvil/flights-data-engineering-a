"""Modelos SQLAlchemy 2.0 para el schema relacional del proyecto flights."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base declarativa de SQLAlchemy."""


class Airline(Base):
    """Catálogo de aerolíneas."""

    __tablename__ = "airlines"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airline: Mapped[str] = mapped_column(String(255), nullable=False)


class Airport(Base):
    """Catálogo de aeropuertos."""

    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airport: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class Flight(Base):
    """Tabla principal de vuelos.

    Se usa un surrogate key `flight_id` porque el dataset no trae una llave primaria natural
    única y estable para cada observación.
    """

    __tablename__ = "flights"

    flight_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    day: Mapped[int] = mapped_column(Integer, nullable=False)

    airline: Mapped[Optional[str]] = mapped_column(
        String(10),
        ForeignKey("airlines.iata_code"),
        nullable=True,
    )
    flight_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    origin_airport: Mapped[Optional[str]] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=True,
    )
    destination_airport: Mapped[Optional[str]] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=True,
    )

    scheduled_departure: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    departure_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    arrival_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    cancelled: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    cancellation_reason: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    distance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    air_system_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    airline_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weather_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    late_aircraft_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    security_delay: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
