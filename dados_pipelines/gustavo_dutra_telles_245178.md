# PROVA - Integracao de Dados e Pipelines (Respondida)

Nome completo: Gustavo Dutra Telles
RA: 245178
Data: 24/04/2026

## Questao 1 - Fundamentos de ETL e Infraestrutura [20 pontos]

### Enunciado
Uma fintech integra 3 fontes para o DW:
- Fonte A: API REST de transacoes bancarias (JSON paginado, ~500 mil/dia, continuo)
- Fonte B: PostgreSQL legado de clientes (10 tabelas normalizadas, ~200 mil, diario)
- Fonte C: Excel mensal de compliance (formato variavel)

### (a) Compare ETL vs ELT para este cenario [max. 200 palavras]

#### Enunciado
(i) Para cada fonte (A, B, C), indique ETL ou ELT e justifique.
(ii) Desenhe o fluxo e mostre onde ocorre a transformacao.
(iii) Em que cenario ELT e pior que ETL, com exemplo da fintech.

#### Resposta
(i) Eu adotaria arquitetura hibrida. Fonte A: ELT (volume alto e atualizacao continua), priorizando ingestao rapida em raw e transformacao incremental no destino. Fonte B: ETL (estrutura relacional conhecida, com joins e regras de negocio antes da carga analitica). Fonte C: ETL (schema variavel em Excel exige validacao e padronizacao previa).

(ii) Fluxos por fonte:

```text
[Fonte A - ELT]
Entrada:
    API REST (JSON paginado)
Processamento:
    1) Extract (Python)
    2) Load RAW (staging DW)
Saida:
    Transform SQL (camada curated)

[Fonte B - ETL]
Entrada:
    PostgreSQL legado (10 tabelas normalizadas)
Processamento:
    1) Extract (queries/JOIN)
    2) Transform (Python/Spark + regras de negocio)
Saida:
    Load DW conformado

[Fonte C - ETL]
Entrada:
    Excel mensal (schema variavel)
Processamento:
    1) Extract (pandas)
    2) Transform (validacao de schema + tipagem + mascaramento de CPF)
Saida:
    Load DW
```

(iii) ELT e pior quando a carga bruta aumenta risco e custo sem ganho. Exemplo: planilhas de compliance com PII e colunas inconsistentes. Carregar direto no raw do DW eleva risco LGPD, aumenta armazenamento de dado sujo e piora performance de query ate a limpeza.

### (b) Idempotencia e Upsert [max. 200 palavras + SQL]

#### Enunciado
(i) Defina idempotencia em pipelines.
(ii) Escreva INSERT ... ON CONFLICT para atualizar amount, status e updated_at.
(iii) Explique a 2a execucao com 1.000 transacoes (800 existentes alteradas, 200 novas).

#### Resposta
(i) Idempotencia significa que reprocessar o mesmo lote nao gera efeito colateral adicional: o estado final permanece consistente, sem duplicatas nem inflacao de metricas. Em producao, e critica porque retries, reexecucoes manuais e falhas parciais sao esperados.

(ii) SQL (PostgreSQL):

```sql
INSERT INTO transactions (transaction_id, amount, status, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (transaction_id)
DO UPDATE SET
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    updated_at = NOW();
```

(iii) Na segunda execucao, as 800 transacoes existentes entram em DO UPDATE: amount e status sao sobrescritos pelos novos valores da API e updated_at recebe o timestamp atual. As 200 transacoes novas entram como INSERT. Resultado: +200 linhas no total, sem duplicar transaction_id. Se updated_at viesse da API, no update seria EXCLUDED.updated_at.

### (c) Docker Compose e reprodutibilidade [max. 150 palavras]

#### Enunciado
(i) Explique depends_on com condition: service_healthy.
(ii) Risco de nao usar volume no PostgreSQL e efeito do docker compose down.
(iii) Por que .env deve ficar fora do codigo e do versionamento.

#### Resposta
(i) depends_on com condition: service_healthy faz o app Python esperar o PostgreSQL ficar realmente pronto. Sem essa condicao, o Compose garante apenas container running, nao servico pronto para conexao. Como o Postgres demora alguns segundos para inicializar, o app pode falhar no bootstrap. O gatilho de prontidao vem do healthcheck (pg_isready).

(ii) Sem volume, os dados ficam no filesystem efemero do container. Ao remover o container (docker compose down), o banco volta vazio na proxima subida. Com volume nomeado, os dados sobrevivem a recriacao do container.

(iii) .env fora do codigo evita hardcode de segredo e separa configuracao por ambiente (dev/hml/prod). Fora do Git, evita vazamento de credenciais e reduz risco de acesso indevido a dados sensiveis.

---

## Questao 2 - Ecossistema Big Data e Arquiteturas de Dados [25 pontos]

### Enunciado
Rede de supermercados (200 lojas) com 4 fontes:
- PDVs: ~2 milhoes/dia (estruturado)
- Cameras: 50 TB/mes (nao estruturado)
- Redes sociais: streaming (semi-estruturado)
- IoT: ~5,7 milhoes/dia (serie temporal)

### (a) NoSQL por fonte [max. 80 palavras por fonte, total 320]

#### Enunciado
Para cada fonte: (i) familia NoSQL e banco, (ii) justificativa por acesso/estrutura, (iii) por que relacional nao e ideal.

#### Resposta
PDVs: Colunar (Apache Cassandra). Escrita massiva por chave composta (loja, data, cupom) e leitura por janela temporal/loja funcionam bem em wide-column. Relacional escala verticalmente com mais custo e pode degradar em picos de escrita distribuida.

Cameras: Chave-valor (DynamoDB) para metadados e ponteiros de arquivo; o video bruto fica em object storage (S3/Data Lake). Padrao de acesso: buscar camera+timestamp e recuperar URI. Relacional nao e adequado para payload binario massivo (50 TB/mes), com alto custo de armazenamento e baixa eficiencia de ingestao/consulta.

Redes sociais: Documento (Elasticsearch). Padrao de acesso principal e ingestao em streaming com busca full-text por hashtag/termo, filtros por tempo/canal e agregacao de sentimento. Relacional tende a ter schema rigido para JSON variavel e menor eficiencia em full-text em alta taxa de escrita.

IoT: Colunar (HBase/Bigtable). Leitura por sensor e intervalo de tempo, com append continuo e alta cardinalidade. Wide-column oferece particionamento horizontal e baixa latencia para series temporais. Relacional sofre com manutencao de indice e gargalo de escrita em volume de milhoes de eventos/dia.

### (b) Arquitetura Lambda vs Kappa [max. 230 palavras + diagrama]

#### Enunciado
(i) Desenhe Lambda com Batch, Speed e Serving.
(ii) Mostre fluxo de vendas batch e IoT streaming com tecnologias.
(iii) Avalie se Kappa (sem batch) e viavel neste cenario.

#### Resposta
Lambda separa processamento historico e tempo real:
- Batch Layer: guarda historico completo e recalcula visoes exatas.
- Speed Layer: processa eventos recentes com baixa latencia.
- Serving Layer: unifica visao batch + speed para consulta.

Diagrama textual:

```text
+------------------------- LAMBDA ARCHITECTURE --------------------------+
| ENTRADA                                                               |
|   Batch: PDVs (fim do dia)                                            |
|   Streaming: IoT (tempo real)                                         |
|                                                                       |
| PROCESSAMENTO                                                          |
|   Batch Layer: Data Lake (HDFS/S3) -> Spark Batch                     |
|   Speed Layer: Kafka -> Spark Structured Streaming / Flink             |
|                                                                       |
| SAIDA                                                                  |
|   Serving Layer: HBase / Delta / Redis -> BI / API                    |
|   (visao unificada de batch + speed)                                  |
+------------------------------------------------------------------------+
```

Fluxo: vendas de PDV entram no batch noturno para metricas consolidadas (faturamento, margem). Sensores IoT entram em streaming para alertas de temperatura em minutos. Serving entrega dashboards unificados.

Kappa e parcialmente viavel, mas eu nao eliminaria batch aqui. Trade-off: Kappa reduz duplicacao de codigo (uma via), porem exige reter historico longo no broker para replay. Exemplo: reprocessar 6 meses de PDV exigiria retencao extensa em Kafka e processamento demorado no stream engine. No varejo, com fechamento diario, ajuste retroativo e auditoria fiscal, batch noturno em lake tende a ser mais barato e previsivel.

### (c) Data Lake, Data Warehouse e Data Swamp [max. 200 palavras]

#### Enunciado
(i) Diferencie Lake vs Warehouse em 3 dimensoes exatas.
(ii) Defina Data Swamp com 3 praticas que causam degradacao.
(iii) Proponha 3 medidas de governanca, explicando como cada uma corrige um problema.

#### Resposta
(i) Tres dimensoes:
1) Schema: Data Lake = schema-on-read; Data Warehouse = schema-on-write.
2) Tipos de dados: Lake suporta estruturado, semi e nao estruturado; Warehouse privilegia estruturado modelado.
3) Usuario principal: Lake atende engenharia/ciencia de dados; Warehouse atende BI/negocio.

(ii) Um Lake vira Swamp quando perde descobribilidade e confianca. Tres praticas que causam isso: (1) ingestao sem catalogo/metadados, (2) ausencia de padrao de particao/nomeacao/versionamento, (3) falta de controles de qualidade e ownership por dominio.

(iii) Medidas com relacao direta problema -> solucao:
1) Problema: ingestao sem catalogo. Medida: catalogo obrigatorio (Glue/Hive + owner + linhagem) antes de publicar dataset; isso torna dado descobrivel e auditavel.
2) Problema: ausencia de padrao estrutural. Medida: contrato de schema + particao + versionamento (Delta/Iceberg); isso evita caos de formatos e quebras de leitura.
3) Problema: falta de controle de qualidade. Medida: data quality gates (nulos, ranges, duplicidade, SLA) bloqueando promocao para trusted; isso impede que dado ruim vire base oficial.

---

## Questao 3 - Depuracao de DAG: Apache Airflow [20 pontos]

### Enunciado
A DAG de ETL Rest Countries roda sem erro de sintaxe, mas: tasks fora de ordem, duplicacao no banco e centenas de runs ao ativar. Identificar 5 erros e corrigir.

### (a) Identifique e corrija 5 erros [max. 50 palavras por erro; total 250 + codigo]

#### Enunciado
Para cada erro: (i) linha/trecho, (ii) por que esta errado e impacto, (iii) codigo corrigido.

#### Resposta
Erro 1
- Linha/trecho: linha 12, catchup=True.
- Problema/impacto: com start_date antigo e @daily, o scheduler cria backlog (centenas de DAG runs).
- Correcao: catchup=False.

Erro 2
- Linha/trecho: linha 49, python_callable=transform com def transform(data).
- Problema/impacto: PythonOperator nao passa retorno anterior como argumento posicional; pode ocorrer TypeError (argumento ausente) ou processamento com input None.
- Correcao: def transform(**context) + ti.xcom_pull(task_ids="extract").

Erro 3
- Linha/trecho: linha 51, python_callable=load com def load(records).
- Problema/impacto: mesmo erro de passagem de dados; a carga pode falhar por assinatura incorreta ou executar sem registros validos.
- Correcao: def load(**context) + ti.xcom_pull(task_ids="transform").

Erro 4
- Linha/trecho: linhas 38-39, INSERT sem ON CONFLICT.
- Problema/impacto: toda reexecucao reinsere os mesmos paises e gera duplicidade. Alem disso, ON CONFLICT so funciona sobre PK/UNIQUE existente.
- Correcao: criar constraint unica (ex. UNIQUE em name) e usar INSERT ... ON CONFLICT ... DO UPDATE.

Erro 5
- Linha/trecho: linha 52, ausencia de dependencia entre tasks.
- Problema/impacto: scheduler pode executar em ordem indefinida.
- Correcao: extract_task >> transform_task >> load_task.

Codigo corrigido (PythonOperator):

```python
from datetime import datetime

import psycopg2
import requests
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

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_countries_name
        ON countries (name)
        """
    )

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
```

### (b) Reescreva a DAG com TaskFlow API [max. 150 palavras + codigo]

#### Enunciado
(i) Codigo completo com @dag e @task.
(ii) Explique XCom implicito vs explicito.
(iii) Limite de XCom para grande volume e alternativa.

#### Resposta
Na TaskFlow, retorno de funcao vira XCom push implicito e parametro vira pull implicito. No PythonOperator classico, isso e explicito via ti.xcom_push/ti.xcom_pull. Na TaskFlow, o Airflow serializa automaticamente o retorno para armazenar no metastore.

XCom nao deve carregar payload grande: aumenta I/O do metastore e impacta scheduler/webserver. Quando o volume crescer (ex. muitos campos por pais), grave o dataset em storage externo (S3/GCS/MinIO/staging) e passe por XCom apenas ponteiros (path, chave, id_lote).

```python
from datetime import datetime

import psycopg2
import requests
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

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_countries_name
            ON countries (name)
            """
        )

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
```

---

## Questao 4 - Pipeline ETL: Modelagem e Implementacao [20 pontos]

### Enunciado
Pipeline Banco Mundial para tabelas countries, indicators e wdi_facts.

### (a) DDL completo das 3 tabelas [max. 100 palavras + SQL]

#### Enunciado
(i) Defina countries com tipos, PK e restricoes.
(ii) Defina wdi_facts com PK composta e FKs.
(iii) Justifique PK (iso2_code, indicator_code, year).

#### Resposta
A PK composta representa a granularidade real do fato: um pais tem varios indicadores ao longo de varios anos. Se a PK fosse apenas iso2_code, haveria uma linha por pais, com perda de historico e sobrescrita indevida. Na tabela indicators, as colunas derivam do JSON do enunciado (id, value e unit).

```sql
-- Tabela de paises
CREATE TABLE IF NOT EXISTS countries (
    iso2_code CHAR(2) PRIMARY KEY,
    iso3_code CHAR(3) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    region VARCHAR(80),
    income_group VARCHAR(60),
    capital VARCHAR(80),
    longitude NUMERIC(9,4),
    latitude NUMERIC(9,4),
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- Tabela de indicadores
CREATE TABLE IF NOT EXISTS indicators (
    indicator_code VARCHAR(40) PRIMARY KEY,
    indicator_name TEXT NOT NULL,
    unit VARCHAR(30)
);

-- Tabela fato (PK composta)
CREATE TABLE IF NOT EXISTS wdi_facts (
    iso2_code CHAR(2) REFERENCES countries(iso2_code),
    indicator_code VARCHAR(40) REFERENCES indicators(indicator_code),
    year SMALLINT NOT NULL,
    value NUMERIC(18,4),
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (iso2_code, indicator_code, year)
);
```

### (b) Transformacao (T1, T2, T3) [max. 150 palavras + codigo]

#### Enunciado
(i) is_real_country(record)
(ii) safe_float(value)
(iii) filtro de year entre 2010 e 2025

#### Resposta
As funcoes abaixo removem agregados regionais, convertem tipos sem quebrar o pipeline e aplicam o recorte temporal. Em T1, isalpha() e isupper() elimina codigos agregados como 1W (nao pais real). Em T3, o campo date da API vem como string (ex. "2022"), por isso a conversao para int e obrigatoria antes do filtro.

```python
def is_real_country(record: dict) -> bool:
    # Pais real: ISO2 com exatamente 2 letras maiusculas.
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
```

### (c) Carga e validacao [max. 150 palavras + SQL/codigo]

#### Enunciado
(i) Upsert com SQLAlchemy em wdi_facts.
(ii) Por que countries/indicators antes de wdi_facts.
(iii) Duas queries SQL de validacao.

#### Resposta
A carga de dimensoes primeiro garante integridade referencial: wdi_facts depende de FKs para countries e indicators. Se inverter a ordem, inserts de fatos falham por FK inexistente (ou criam orfaos sem FK), comprometendo consistencia analitica.

```python
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
```

```sql
-- Q1: volume por indicador
SELECT indicator_code, COUNT(*) AS total
FROM wdi_facts
GROUP BY indicator_code
ORDER BY indicator_code;

-- Q2: idempotencia operacional (rodar antes e depois da 2a execucao)
SELECT COUNT(*) AS total_rows
FROM wdi_facts;
```

---

## Questao 5 - Questao Integradora (Ensaio) [15 pontos, max. 500 palavras]

### Enunciado
Operadora de saude com fontes: HIS Oracle (500+ tabelas), API de laboratorios (~50 mil/dia), planilhas ANS trimestrais. Redigir parecer cobrindo arquitetura, ETL/ELT por fonte, orquestracao e governanca/LGPD.

### Resposta
Eu recomendaria Lakehouse (S3/ADLS + Delta Lake + Spark + Airflow), nao DW puro nem Lake puro. Motivo: combinar historico granular para auditoria clinica, ingestao heterogenea (Oracle, REST, Excel) e consumo BI confiavel. Camadas: Bronze (raw imutavel), Silver (padronizada/qualificada), Gold (modelo de indicadores). HIS entra em Bronze como CDC em parquet/delta; API labs em JSON particionado por data/hora; ANS em arquivos versionados por competencia.

Estrategia por fonte:
1) HIS Oracle: CDC por redo log (Debezium/GoldenGate), nao full diario. Transformacoes na Silver: padronizacao CID/TUSS, dedupe por atendimento+evento, normalizacao de unidade/tempo, regras de completude. Carga por MERGE/UPSERT idempotente com chave natural + watermark.
2) API laboratorios: incremental por updated_at + paginacao. Transformacoes: validacao de schema, range biologico, timezone unico, dedupe por id_exame+data_coleta+laboratorio. Carga por upsert idempotente.
3) ANS Excel: full trimestral (baixo volume, alto schema drift). ETL na borda: parsing, dicionario de mapeamento, reconciliacao de versoes e controle de qualidade antes da Bronze.

Integracao entre topicos: a decisao Bronze/Silver/Gold guia a DAG do Airflow. Tasks de ingestao alimentam Bronze; transformacoes e qualidade promovem para Silver; so dados validados sobem para Gold. LGPD segue o mesmo fluxo: dado sensivel restrito na Bronze, pseudonimizacao na Silver e exposicao anonima na Gold analitica.

Orquestracao: Airflow com DAG principal:

```text
ENTRADA
    start
        +-- ingest_his_cdc
        +-- ingest_labs_api
        +-- ingest_ans_excel

PROCESSAMENTO
    ingest_his_cdc   -> silver_his   --+
    ingest_labs_api  -> silver_labs  --+--> quality_checks
    ingest_ans_excel -> silver_ans   --+

SAIDA
    quality_checks -> publish_dashboards -> end
```

Schedule: HIS a cada 15 min; labs a cada 5 min; ANS trimestral. Retry policy: 3 tentativas com backoff exponencial, SLA monitoring, backfill controlado e DAG parametrizada por periodo/lote. Alertas: Slack/Teams + email on-failure/on-SLA-miss. Cron sozinho e insuficiente: se a task de labs falhar as 03:00, o fluxo para e o painel atrasa sem governanca de retry/dependencia; no Airflow, retries, dependencias e alertas tratam isso automaticamente.

LGPD: pseudonimizar CPF/CNS/nome/telefone/endereco na Silver com tokenizacao e token vault (KMS/HSM). Na Gold, anonimizar quando identificacao individual nao for necessaria. Retencao: prazo legal minimo para raw sensivel + descarte automatizado; agregados anonimos com prazo maior. Acesso: RBAC/ABAC, column-level security, data masking dinamico, criptografia em repouso/transito e segregacao por ambiente. Auditabilidade: logs imutaveis de acesso, linhagem campo-a-campo e trilha de alteracao de regra (quem/quando/ticket).

---

## Evidencias de execucao

### 1) SQL validado em PostgreSQL (container wb_postgres)

Comandos executados:
- criacao de transactions_validation + upsert de 1.000 linhas sobre base inicial de 800
- criacao do schema prova_q4 + DDL de countries/indicators/wdi_facts
- dois inserts no mesmo fato com ON CONFLICT para validar update por PK composta

Saidas principais:

```text
before_count = 800
after_count = 1000
updated_existing_800 = 800
inserted_new_200 = 200

updated_value_after_conflict = 9000.0000
total_rows = 1
distinct_keys = 1
```

Interpretacao:
- Q1(b): comprovado comportamento pedido (800 updates + 200 inserts, sem duplicar PK)
- Q4(a/c): comprovado DDL valido e upsert funcional na chave composta

### 2) Python validado sem venv (Python 3.13 global)

Arquivo executado:
- dados_pipelines/validar_snippets_prova.py

Saida:

```text
[OK] Sintaxe dos snippets validada.
[OK] Testes funcionais das funcoes de transformacao passaram.
```

Interpretacao:
- sintaxe dos blocos Python da prova esta valida
- funcoes T1/T2/T3 da Questao 4(b) executaram e passaram nos testes