from typing import Any, Dict, List

from sqlalchemy import CHAR, DateTime, ForeignKey, Numeric, String, Text, create_engine, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from config import settings


class Base(DeclarativeBase):
    pass


class Country(Base):
    __tablename__ = "countries"

    iso2_code: Mapped[str] = mapped_column(CHAR(2), primary_key=True)
    iso3_code: Mapped[str | None] = mapped_column(CHAR(3), nullable=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    region: Mapped[str | None] = mapped_column(String(80), nullable=True)
    income_group: Mapped[str | None] = mapped_column(String(60), nullable=True)
    capital: Mapped[str | None] = mapped_column(String(80), nullable=True)
    longitude: Mapped[float | None] = mapped_column(Numeric(9, 4), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Numeric(9, 4), nullable=True)
    loaded_at: Mapped[Any] = mapped_column(DateTime, server_default=func.now())


class Indicator(Base):
    __tablename__ = "indicators"

    indicator_code: Mapped[str] = mapped_column(String(40), primary_key=True)
    indicator_name: Mapped[str] = mapped_column(Text, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(30), nullable=True)


class WdiFact(Base):
    __tablename__ = "wdi_facts"

    iso2_code: Mapped[str] = mapped_column(CHAR(2), ForeignKey("countries.iso2_code"), primary_key=True)
    indicator_code: Mapped[str] = mapped_column(
        String(40), ForeignKey("indicators.indicator_code"), primary_key=True
    )
    year: Mapped[int] = mapped_column(nullable=False, primary_key=True)
    value: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    loaded_at: Mapped[Any] = mapped_column(DateTime, server_default=func.now())


def _get_engine():
    return create_engine(settings.sqlalchemy_database_url, pool_pre_ping=True, future=True)


def _upsert_countries(session: Session, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("[load] countries vazio, nada para carregar.")
        return

    stmt = pg_insert(Country)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Country.iso2_code],
        set_={
            "iso3_code": stmt.excluded.iso3_code,
            "name": stmt.excluded.name,
            "region": stmt.excluded.region,
            "income_group": stmt.excluded.income_group,
            "capital": stmt.excluded.capital,
            "longitude": stmt.excluded.longitude,
            "latitude": stmt.excluded.latitude,
            "loaded_at": func.now(),
        },
    )
    session.execute(stmt, rows)


def _upsert_indicators(session: Session, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("[load] indicators vazio, nada para carregar.")
        return

    stmt = pg_insert(Indicator)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Indicator.indicator_code],
        set_={
            "indicator_name": stmt.excluded.indicator_name,
            "unit": stmt.excluded.unit,
        },
    )
    session.execute(stmt, rows)


def _upsert_facts(session: Session, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("[load] wdi_facts vazio, nada para carregar.")
        return

    stmt = pg_insert(WdiFact)
    stmt = stmt.on_conflict_do_update(
        index_elements=[WdiFact.iso2_code, WdiFact.indicator_code, WdiFact.year],
        set_={
            "value": stmt.excluded.value,
            "loaded_at": func.now(),
        },
    )
    session.execute(stmt, rows)


def load_all(
    countries_rows: List[Dict[str, Any]],
    indicators_rows: List[Dict[str, Any]],
    facts_rows: List[Dict[str, Any]],
) -> None:
    engine = _get_engine()

    with Session(engine) as session:
        with session.begin():
            _upsert_countries(session, countries_rows)
        with session.begin():
            _upsert_indicators(session, indicators_rows)
        with session.begin():
            _upsert_facts(session, facts_rows)

    print(f"[load] countries carregados: {len(countries_rows)}")
    print(f"[load] indicators carregados: {len(indicators_rows)}")
    print(f"[load] wdi_facts carregados: {len(facts_rows)}")
