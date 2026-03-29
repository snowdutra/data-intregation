# ETL World Bank API v2

## Visao geral
Este projeto implementa um pipeline ETL para montar uma base relacional de indicadores socioeconomicos do Banco Mundial.

Problema resolvido:
- carregar paises filtrados por grupos de renda LIC, MIC e HIC
- carregar 5 indicadores obrigatorios da atividade
- manter serie historica em banco PostgreSQL com reprocessamento idempotente

API utilizada:
- endpoint de paises: /v2/country
- endpoint de indicadores: /v2/country/all/indicator/{id}

Indicadores obrigatorios implementados:
- NY.GDP.PCAP.KD
- SP.POP.TOTL
- SH.XPD.CHEX.GD.ZS
- SE.XPD.TOTL.GD.ZS
- EG.ELC.ACCS.ZS

## Modelo de dados
As 3 tabelas estao definidas em db/init.sql.

### countries
- iso2_code CHAR(2) PK
- iso3_code CHAR(3)
- name VARCHAR(100) NOT NULL
- region VARCHAR(80)
- income_group VARCHAR(60)
- capital VARCHAR(80)
- longitude NUMERIC(9,4)
- latitude NUMERIC(9,4)
- loaded_at TIMESTAMP DEFAULT NOW()

### indicators
- indicator_code VARCHAR(40) PK
- indicator_name TEXT NOT NULL
- unit VARCHAR(30)

### wdi_facts
- iso2_code CHAR(2) FK -> countries.iso2_code
- indicator_code VARCHAR(40) FK -> indicators.indicator_code
- year SMALLINT NOT NULL
- value NUMERIC(18,4)
- loaded_at TIMESTAMP DEFAULT NOW()
- PK composta: (iso2_code, indicator_code, year)

Abordagem escolhida: ORM com DeclarativeBase no load.py.
Justificativa: a atividade exige SQLAlchemy e upsert nas 3 tabelas; com ORM foi possivel manter mapeamento explicito, legibilidade da regra de upsert e reutilizar as entidades nas clausulas on_conflict_do_update.

## Regras de transformacao
As regras T1-T5 estao implementadas em src/transform.py.

- T1 Filtro de entidade: descarta registros cujo ISO2 nao e pais real (2 letras maiusculas) e tambem remove fatos fora da lista de paises validos apos filtro de renda.
- T2 Limpeza de strings: aplica strip em strings, converte vazio para None e normaliza regiao para title-case.
- T3 Conversao de tipos: year para int, value para float com tratamento seguro, longitude/latitude para float.
- T4 Filtro temporal: mantem apenas anos entre 2010 e ano corrente configurado.
- T5 Deduplicacao: remove duplicatas por (iso2, indicator_code, year), mantendo a versao mais recente encontrada e registrando quantidade em log.

## Como executar

1. Clonar o repositorio:

```bash
git clone https://github.com/snowdutra/data-intregation.git
```

2. Entrar na pasta do projeto:
```bash
cd etl_bancomundial
```

3. Preparar variaveis de ambiente:

Linux/macOS:
```bash
cp .env.example .env
```

Windows CMD:
```cmd
copy .env.example .env
```

Windows PowerShell:
```powershell
Copy-Item .env.example .env
```

Se quiser trocar credenciais, edite DB_NAME, DB_USER e DB_PASSWORD no .env antes de subir os containers.

4. Subir todo o ambiente com um unico comando (recomendado):

```bash
docker compose up --build
```

Opcional para execucao em duas etapas:
- subir apenas banco: `docker compose up -d postgres`
- executar ETL: `docker compose run --rm etl_app`

5. Acessar banco para validacao:

```bash
docker exec -it wb_postgres psql -U <DB_USER> -d <DB_NAME>
```

## Consultas de validacao
As queries abaixo foram executadas apos a primeira execucao completa.

### 1) Volume de paises

```sql
SELECT COUNT(*) FROM countries;
```

Saida real:

```text
 count
-------
   215
(1 row)
```

### 2) Distribuicao por renda

```sql
SELECT income_group, COUNT(*)
FROM countries
GROUP BY income_group
ORDER BY 2 DESC;
```

Saida real:

```text
 income_group | count
--------------+-------
 MIC          |   104
 HIC          |    86
 LIC          |    25
(3 rows)
```

### 3) Volume e nulos por indicador

```sql
SELECT indicator_code,
       COUNT(*) as obs,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as nulls
FROM wdi_facts
GROUP BY indicator_code
ORDER BY indicator_code;
```

Saida real:

```text
  indicator_code   | obs  | nulls
-------------------+------+-------
 EG.ELC.ACCS.ZS    | 2150 |    20
 NY.GDP.PCAP.KD    | 2150 |   105
 SE.XPD.TOTL.GD.ZS | 2150 |   843
 SH.XPD.CHEX.GD.ZS | 2150 |   413
 SP.POP.TOTL       | 2150 |     0
(5 rows)
```

### 4) PIB per capita de 5 paises

```sql
SELECT c.name, f.year, f.value
FROM wdi_facts f
JOIN countries c ON c.iso2_code = f.iso2_code
WHERE f.indicator_code = 'NY.GDP.PCAP.KD'
  AND c.iso2_code IN ('BR','US','CN','DE','NG')
ORDER BY c.name, f.year;
```

Saida real (resumo): 50 linhas retornadas (5 paises x 10 anos).

Trecho:

```text
Brazil        | 2015 |  8936.1956
...
Brazil        | 2024 |  9566.7441
China         | 2015 |  8175.3329
...
United States | 2024 | 66356.1707
(50 rows)
```

### 5) Idempotencia apos segunda execucao

Antes da segunda execucao:

```sql
SELECT COUNT(*) AS before_count FROM wdi_facts;
```

Saida real:

```text
 before_count
--------------
        10750
(1 row)
```

Depois da segunda execucao:

```sql
SELECT COUNT(*) AS after_count FROM wdi_facts;
```

Saida real:
```text
 after_count
-------------
       10750
(1 row)
```

Resultado: contagem identica, sem duplicacao.

## Decisões tecnicas

1. Foi usada API /country para dimensao de paises e /country/all/indicator/{id} para fatos, mantendo separacao clara entre dimensoes e fato.

2. O filtro de renda mapeia LMC e UMC para MIC, para aderir ao recorte LIC/MIC/HIC pedido no enunciado.

3. A carga foi implementada com SQLAlchemy ORM + postgres insert on_conflict_do_update, sem uso de psycopg2 direto no load.py.

4. O upsert foi executado em lote via session.execute(stmt, lista), evitando loop de insert registro a registro.

5. Cada tabela foi carregada em bloco transacional proprio com session.begin(), garantindo rollback automatico por tabela em caso de falha.

6. O filtro temporal foi parametrizado por MIN_YEAR e MAX_YEAR para facilitar reproducao e auditoria.

7. A chave composta em wdi_facts garante idempotencia e foi validada por reexecucao completa do pipeline.

## Checklist de aderencia ao enunciado
- [x] ETL completo em camadas separadas (extract, transform, load)
- [x] Uso da API do Banco Mundial v2
- [x] Carga dos 5 indicadores obrigatorios
- [x] Filtro de paises por grupos de renda LIC, MIC e HIC
- [x] Regras de transformacao T1-T5 implementadas
- [x] Persistencia em PostgreSQL com 3 tabelas relacionais
- [x] Uso de SQLAlchemy na carga
- [x] Upsert com idempotencia nas 3 tabelas
- [x] Execucao via Docker Compose
- [x] Evidencias de validacao com consultas SQL e segunda execucao

Entregaveis incluidos no repositorio:
- codigo-fonte em src/
- script SQL de criacao das tabelas em db/init.sql
- docker-compose.yml e Dockerfile
- requirements.txt
- README com instrucoes, consultas e evidencias
- .env.example para reproducao da execucao
