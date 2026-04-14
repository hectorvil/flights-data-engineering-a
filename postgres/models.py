"""Schema PostgreSQL del dataset de vuelos usando SQLAlchemy 2.0."""

from typing import Optional

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airline: Mapped[str] = mapped_column(String(255), nullable=False)


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airport: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)


class Flight(Base):
    __tablename__ = "flights"

    flight_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    day: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    airline: Mapped[Optional[str]] = mapped_column(
        String(10), ForeignKey("airlines.iata_code"), nullable=True
    )

    flight_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    origin_airport: Mapped[Optional[str]] = mapped_column(
        String(10), ForeignKey("airports.iata_code"), nullable=True
    )

    destination_airport: Mapped[Optional[str]] = mapped_column(
        String(10), ForeignKey("airports.iata_code"), nullable=True
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