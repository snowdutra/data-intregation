import textwrap


def compile_snippet(name: str, code: str) -> None:
    compile(textwrap.dedent(code), filename=name, mode="exec")


python_operator_dag_code = '''
from datetime import datetime
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def extract(**context):
    response = requests.get("https://restcountries.com/v3.1/all", timeout=30)
    response.raise_for_status()
    context["ti"].xcom_push(key="raw_countries", value=response.json())


def transform(**context):
    data = context["ti"].xcom_pull(task_ids="extract", key="raw_countries")
    cleaned = []
    for country in data:
        cleaned.append(
            {
                "name": country.get("name", {}).get("common"),
                "region": country.get("region"),
                "population": country.get("population"),
            }
        )
    context["ti"].xcom_push(key="clean_countries", value=cleaned)


def load(**context):
    records = context["ti"].xcom_pull(task_ids="transform", key="clean_countries")
    conn = psycopg2.connect(
        host="etl_postgres", dbname="etl_db", user="etl_user", password="etl_pass"
    )
    cur = conn.cursor()
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_countries_name ON countries (name)")
    for r in records:
        cur.execute(
            """
            INSERT INTO countries (name, region, population)
            VALUES (%s, %s, %s)
            ON CONFLICT (name)
            DO UPDATE SET
                region = EXCLUDED.region,
                population = EXCLUDED.population
            """,
            (r["name"], r["region"], r["population"]),
        )
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="etl_countries",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
'''


taskflow_dag_code = '''
from datetime import datetime
import requests
import psycopg2
from airflow.decorators import dag, task


@dag(
    dag_id="etl_countries_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def etl_countries_taskflow():
    @task
    def extract():
        r = requests.get("https://restcountries.com/v3.1/all", timeout=30)
        r.raise_for_status()
        return r.json()

    @task
    def transform(data):
        return [
            {
                "name": c.get("name", {}).get("common"),
                "region": c.get("region"),
                "population": c.get("population"),
            }
            for c in data
        ]

    @task
    def load(records):
        conn = psycopg2.connect(
            host="etl_postgres", dbname="etl_db", user="etl_user", password="etl_pass"
        )
        cur = conn.cursor()
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_countries_name ON countries (name)")
        for r in records:
            cur.execute(
                """
                INSERT INTO countries (name, region, population)
                VALUES (%s, %s, %s)
                ON CONFLICT (name)
                DO UPDATE SET
                    region = EXCLUDED.region,
                    population = EXCLUDED.population
                """,
                (r["name"], r["region"], r["population"]),
            )
        conn.commit()
        cur.close()
        conn.close()

    load(transform(extract()))


dag = etl_countries_taskflow()
'''


sqlalchemy_upsert_code = """
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert


def upsert_wdi_facts(session, rows, WdiFact):
    stmt = pg_insert(WdiFact)
    stmt = stmt.on_conflict_do_update(
        index_elements=[WdiFact.iso2_code, WdiFact.indicator_code, WdiFact.year],
        set_={
            "value": stmt.excluded.value,
            "loaded_at": func.now(),
        },
    )
    session.execute(stmt, rows)
"""


transform_functions_code = """
def is_real_country(record: dict) -> bool:
    country = record.get("country") or {}
    iso2 = country.get("id")
    return isinstance(iso2, str) and len(iso2) == 2 and iso2.isalpha() and iso2.isupper()


def safe_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def in_year_window(record: dict, min_year: int = 2010, max_year: int = 2025) -> bool:
    try:
        year = int(record.get("date"))
    except (TypeError, ValueError):
        return False
    return min_year <= year <= max_year
"""


# 1) Valida sintaxe dos blocos de codigo da prova.
compile_snippet("q3_python_operator_dag", python_operator_dag_code)
compile_snippet("q3_taskflow_dag", taskflow_dag_code)
compile_snippet("q4_sqlalchemy_upsert", sqlalchemy_upsert_code)
compile_snippet("q4_transform_functions", transform_functions_code)
print("[OK] Sintaxe dos snippets validada.")


# 2) Executa testes funcionais das funcoes puras da Questao 4(b).
namespace = {}
exec(textwrap.dedent(transform_functions_code), namespace)

is_real_country = namespace["is_real_country"]
safe_float = namespace["safe_float"]
in_year_window = namespace["in_year_window"]

assert is_real_country({"country": {"id": "BR"}}) is True
assert is_real_country({"country": {"id": "WLD"}}) is False
assert is_real_country({"country": {"id": None}}) is False

assert safe_float("123.45") == 123.45
assert safe_float(10) == 10.0
assert safe_float(None) is None
assert safe_float("abc") is None

assert in_year_window({"date": "2010"}) is True
assert in_year_window({"date": "2025"}) is True
assert in_year_window({"date": "2009"}) is False
assert in_year_window({"date": "x"}) is False

print("[OK] Testes funcionais das funcoes de transformacao passaram.")
