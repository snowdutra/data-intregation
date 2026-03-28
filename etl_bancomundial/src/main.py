from extract import extract_all
from load import load_all
from transform import transform_all


def run_etl() -> None:
    print("[main] iniciando pipeline ETL Banco Mundial...")
    countries_raw, facts_raw = extract_all()
    countries_rows, indicators_rows, facts_rows = transform_all(countries_raw, facts_raw)
    load_all(countries_rows, indicators_rows, facts_rows)
    print("[main] pipeline finalizado com sucesso.")


if __name__ == "__main__":
    run_etl()
