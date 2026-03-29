# Projeto ETL com Docker + Python + PostgreSQL

## Objetivo
Consumir dados da API pública Open Brewery DB, transformar os dados e carregar em PostgreSQL.

## Stack
- Python 3.11
- Docker
- Docker Compose
- PostgreSQL

## Estrutura
- `src/extract.py`: extração da API
- `src/transform.py`: limpeza e transformação
- `src/load.py`: carga no banco
- `src/main.py`: orquestração do ETL
- `lecture/lecture.tex`: material teórico em LaTeX

## Como executar

### 1. Entrar na pasta do projeto

```bash
cd etl_python
```

### 2. Preparar variaveis de ambiente

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

### 3. Subir PostgreSQL

```bash
docker compose up -d postgres
```

### 4. Executar o ETL

```bash
docker compose run --rm etl_app
```

### 5. Acessar o banco

```bash
docker exec -it etl_postgres psql -U <DB_USER> -d <DB_NAME>
```

### 6. Consultar dados

```sql
SELECT COUNT(*) FROM breweries;
SELECT * FROM breweries LIMIT 10;
```

## Entregaveis do projeto
- codigo-fonte em src/
- script SQL em db/init.sql
- docker-compose.yml e Dockerfile
- requirements.txt
- README com instrucoes de execucao e validacao
- .env.example para reproducao local

## Melhorias possíveis
- incluir tabela staging
- incluir logs estruturados
- adicionar testes unitários
- agendar execução com cron ou Airflow
- persistir dados brutos em JSON
