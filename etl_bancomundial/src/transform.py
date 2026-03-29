from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from config import settings


MANDATORY_INDICATORS: Dict[str, Dict[str, str]] = {
    "NY.GDP.PCAP.KD": {
        "indicator_name": "PIB per capita (USD constante 2015)",
        "unit": "USD",
    },
    "SP.POP.TOTL": {
        "indicator_name": "Populacao total",
        "unit": "Pessoas",
    },
    "SH.XPD.CHEX.GD.ZS": {
        "indicator_name": "Gasto em saude (% do PIB)",
        "unit": "% PIB",
    },
    "SE.XPD.TOTL.GD.ZS": {
        "indicator_name": "Gasto em educacao (% do PIB)",
        "unit": "% PIB",
    },
    "EG.ELC.ACCS.ZS": {
        "indicator_name": "Acesso a eletricidade (% da populacao)",
        "unit": "%",
    },
}


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_region(value: Optional[str]) -> Optional[str]:
    cleaned = _safe_str(value)
    if cleaned is None:
        return None
    return cleaned.title()


def _map_income_group(income_code: Optional[str]) -> Optional[str]:
    normalized = (_safe_str(income_code) or "").upper()
    if normalized == "LIC":
        return "LIC"
    if normalized in {"LMC", "UMC", "MIC"}:
        return "MIC"
    if normalized == "HIC":
        return "HIC"
    return None


def _is_real_iso2(iso2: Optional[str]) -> bool:
    if not iso2:
        return False
    return len(iso2) == 2 and iso2.isalpha() and iso2.isupper()


def transform_countries(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    countries: List[Dict[str, Any]] = []

    for row in raw_rows:
        iso2 = (_safe_str(row.get("iso2Code")) or "").upper()
        if not _is_real_iso2(iso2):
            continue

        income_group_obj = row.get("incomeLevel") or {}
        income_group = _map_income_group(income_group_obj.get("id"))
        if income_group not in settings.allowed_income_group_list:
            continue

        region_obj = row.get("region") or {}

        countries.append(
            {
                "iso2_code": iso2,
                "iso3_code": (_safe_str(row.get("id")) or "")[:3] or None,
                "name": _safe_str(row.get("name")),
                "region": _normalize_region(region_obj.get("value")),
                "income_group": income_group,
                "capital": _safe_str(row.get("capitalCity")),
                "longitude": _safe_float(row.get("longitude")),
                "latitude": _safe_float(row.get("latitude")),
            }
        )

    countries = [c for c in countries if c["name"]]
    print(f"[transform] countries total={len(countries)}")
    return countries


def transform_facts(
    raw_rows: List[Dict[str, Any]],
    allowed_iso2: set[str],
) -> List[Dict[str, Any]]:
    # Indexa fatos pela chave natural para deduplicar e preparar upsert idempotente.
    deduped: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    duplicates = 0
    skipped = 0
    current_year = datetime.now().year

    for row in raw_rows:
        country = row.get("country") or {}
        indicator = row.get("indicator") or {}

        iso2 = (_safe_str(country.get("id")) or "").upper()
        indicator_code = (_safe_str(indicator.get("id")) or "").upper()
        year = _safe_int(row.get("date"))

        # Mantem apenas paises validos (ISO2 real) presentes na dimensao filtrada.
        if not _is_real_iso2(iso2) or iso2 not in allowed_iso2:
            skipped += 1
            continue

        if indicator_code not in MANDATORY_INDICATORS:
            skipped += 1
            continue

        # Restringe a serie ao intervalo configurado e evita anos futuros.
        if year is None or year < settings.min_year or year > min(settings.max_year, current_year):
            skipped += 1
            continue

        key = (iso2, indicator_code, year)
        fact = {
            "iso2_code": iso2,
            "indicator_code": indicator_code,
            "year": year,
            "value": _safe_float(row.get("value")),
        }

        if key in deduped:
            duplicates += 1
        # Em conflito de chave, preserva a ultima ocorrencia processada.
        deduped[key] = fact

    print(f"[transform] wdi_facts total={len(deduped)}")
    print(f"[transform] wdi_facts descartados={skipped}")
    print(f"[transform] wdi_facts duplicatas_encontradas={duplicates}")
    return list(deduped.values())


def build_indicators() -> List[Dict[str, Any]]:
    indicators: List[Dict[str, Any]] = []
    for code in settings.indicator_code_list:
        if code not in MANDATORY_INDICATORS:
            continue
        meta = MANDATORY_INDICATORS[code]
        indicators.append(
            {
                "indicator_code": code,
                "indicator_name": meta["indicator_name"],
                "unit": meta["unit"],
            }
        )

    print(f"[transform] indicators total={len(indicators)}")
    return indicators


def transform_all(
    countries_raw: List[Dict[str, Any]], facts_raw: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    countries = transform_countries(countries_raw)
    allowed_iso2 = {country["iso2_code"] for country in countries}
    indicators = build_indicators()
    facts = transform_facts(facts_raw, allowed_iso2)
    return countries, indicators, facts
