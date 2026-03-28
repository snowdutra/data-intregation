import time
from typing import Any, Dict, List, Tuple

import requests

from config import settings


def _request_json(url: str, params: Dict[str, Any]) -> Any:
    for attempt in range(1, settings.wb_retry_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=settings.wb_request_timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == settings.wb_retry_attempts:
                raise RuntimeError(f"Falha ao chamar {url}: {exc}") from exc

            wait_seconds = settings.wb_retry_sleep_seconds * (2 ** (attempt - 1))
            print(
                f"[extract] tentativa {attempt}/{settings.wb_retry_attempts} falhou para {url}: {exc}. "
                f"Nova tentativa em {wait_seconds}s..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"Falha inesperada ao chamar {url}")


def _validate_indicator_payload(payload: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Resposta da API de indicadores fora do formato esperado [meta, data].")

    metadata, data = payload[0], payload[1]
    if not isinstance(metadata, dict) or not isinstance(data, list):
        raise ValueError("Estrutura invalida para metadados/dados de indicador.")

    return metadata, data


def _validate_country_payload(payload: Any) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError("Resposta da API de paises fora do formato esperado [meta, data].")

    metadata, data = payload[0], payload[1]
    if not isinstance(metadata, dict) or not isinstance(data, list):
        raise ValueError("Estrutura invalida para metadados/dados de paises.")

    return metadata, data


def extract_countries() -> List[Dict[str, Any]]:
    url = f"{settings.wb_api_base_url}/country"

    params = {
        "format": settings.wb_format,
        "per_page": settings.wb_country_per_page,
        "page": 1,
    }

    first_payload = _request_json(url, params)
    first_meta, first_data = _validate_country_payload(first_payload)

    all_rows: List[Dict[str, Any]] = list(first_data)
    total_pages = min(int(first_meta.get("pages", 1)), settings.wb_max_pages)

    print(f"[extract] countries paginas={total_pages} registros_pagina_1={len(first_data)}")

    for page in range(2, total_pages + 1):
        params["page"] = page
        payload = _request_json(url, params)
        _, rows = _validate_country_payload(payload)
        print(f"[extract] countries pagina={page} registros={len(rows)}")
        all_rows.extend(rows)

    print(f"[extract] countries total_extraido={len(all_rows)}")
    return all_rows


def extract_indicator(indicator_code: str) -> List[Dict[str, Any]]:
    url = f"{settings.wb_api_base_url}/country/all/indicator/{indicator_code}"

    params = {
        "format": settings.wb_format,
        "per_page": settings.wb_indicator_per_page,
        "mrv": settings.wb_mrv,
        "page": 1,
    }

    first_payload = _request_json(url, params)
    first_meta, first_data = _validate_indicator_payload(first_payload)

    all_rows: List[Dict[str, Any]] = list(first_data)
    total_pages = min(int(first_meta.get("pages", 1)), settings.wb_max_pages)

    print(
        f"[extract] indicador={indicator_code} paginas={total_pages} registros_pagina_1={len(first_data)}"
    )

    for page in range(2, total_pages + 1):
        params["page"] = page
        payload = _request_json(url, params)
        _, rows = _validate_indicator_payload(payload)
        print(f"[extract] indicador={indicator_code} pagina={page} registros={len(rows)}")
        all_rows.extend(rows)

    print(f"[extract] indicador={indicator_code} total_extraido={len(all_rows)}")
    return all_rows


def extract_all() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    countries_raw = extract_countries()

    facts_raw: List[Dict[str, Any]] = []
    for indicator_code in settings.indicator_code_list:
        rows = extract_indicator(indicator_code)
        facts_raw.extend(rows)

    print(f"[extract] total fatos extraidos={len(facts_raw)}")
    return countries_raw, facts_raw
