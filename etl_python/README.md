# Projeto ETL com Docker, Python e PostgreSQL

## Visao geral
Este projeto implementa um pipeline ETL para consumir dados da Open Brewery DB API, transformar os registros e carregar o resultado em PostgreSQL.

Problema resolvido:
- Extrair dados paginados da API publica Open Brewery DB.
- Aplicar limpeza e padronizacao dos campos.
- Carregar os dados em lote no PostgreSQL.

## Stack
- Python 3.11
- Docker
- Docker Compose
- PostgreSQL

## Estrutura
- `src/extract.py`: extracao dos dados da API
- `src/transform.py`: limpeza e transformacao
- `src/load.py`: carga no banco
- `src/main.py`: orquestracao do ETL
- `lecture/lecture.tex`: material teorico em LaTeX

## Como executar

1. Clonar o repositorio:

```bash
git clone https://github.com/snowdutra/data-intregation.git
```

2. Entrar na pasta clonada e abrir o projeto:

```bash
cd data-intregation/etl_python
code .
```

Se nao usar VS Code, execute apenas o comando abaixo e continue no terminal:

```bash
cd data-intregation/etl_python
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
- Subir apenas banco: `docker compose up -d postgres`
- Executar ETL: `docker compose run --rm etl_app`

5. Acessar o banco para validacao:

```bash
docker exec -it etl_postgres psql -U <DB_USER> -d <DB_NAME>
```

6. Consultar dados carregados:

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

## Melhorias possiveis
- Incluir tabela de staging
- Incluir logs estruturados
- Adicionar testes unitarios
- Agendar execucao com cron ou Airflow
- Persistir dados brutos em JSON
