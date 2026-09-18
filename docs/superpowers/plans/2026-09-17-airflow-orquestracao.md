# Orquestração com Apache Airflow: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Orquestrar as três etapas do pipeline_weather em uma DAG do Apache Airflow rodando em Docker a partir da imagem oficial.

**Architecture:** Stack Docker enxuto em `airflow/`, com LocalExecutor e cinco serviços ativos sobre `apache/airflow:3.3.2-python3.12`. O pacote `pipeline_weather` não é instalado na imagem: ele chega por bind mount em `/opt/airflow/projeto`, o que preserva a resolução de `RAIZ_PROJETO` e, com ela, os caminhos de `config/.env`, `.env.local` e `data/`. A DAG usa TaskFlow e chama as funções que o pacote já expõe, com o dado passando de uma task para outra por arquivo.

**Tech Stack:** Apache Airflow 3.3.2, Docker Compose v5.5.1, Postgres 16 para o metadata DB, Python 3.12 no container, pandas, SQLAlchemy e psycopg2 já usados pelo pipeline. O destino da carga continua sendo Lakebase Postgres na Neon.

**Spec:** `docs/superpowers/specs/2026-09-17-airflow-orquestracao-design.md`

## Global Constraints

- Imagem base exata: `apache/airflow:3.3.2-python3.12`.
- Constraints de pip: `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt`. Verificado em 2026-09-17: responde 200 e fixa `pandas==3.0.5`, `SQLAlchemy==2.0.52`, `psycopg2-binary==2.9.13`, `python-dotenv==1.2.3`, `requests==2.34.2`.
- Executor: `LocalExecutor`. Nunca `CeleryExecutor`.
- Serviços ativos, exatamente cinco: `postgres`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`. O `airflow-cli` existe sob o profile `debug` e não sobe com `docker compose up`.
- O pacote `pipeline_weather` não é instalado na imagem. Apenas as cinco dependências do `pyproject.toml`.
- `PYTHONPATH=/opt/airflow/projeto/src` em todos os serviços.
- Única montagem com escrita: `../data` em `/opt/airflow/projeto/data`. As demais são `:ro`.
- Interface web publicada em `8080`.
- DAG: `dag_id` `clima_teresina`, `schedule="@hourly"`, fuso `America/Fortaleza`, `catchup=False`, `max_active_runs=1`.
- Retries por task: `extrair` 3 tentativas com 2 minutos, `transformar` 1 tentativa, `carregar` 3 tentativas com 2 minutos.
- Não criar suíte de testes nem adicionar pytest. A spec coloca isso fora de escopo, e por isso os passos de verificação deste plano são comandos concretos com saída esperada, no lugar do ciclo pytest vermelho-verde.
- Comentários e docstrings em português, seguindo o padrão do repositório.
- `main.py` não é modificado em nenhuma task.

---

### Task 1: Remover os efeitos colaterais de import do pacote

Hoje `extract_data.py`, `transform_data.py` e `load_data.py` executam `sys.stdout.reconfigure(...)` e `logging.basicConfig(...)` ao serem importados. Dentro de uma task do Airflow o `sys.stdout` já foi substituído por um escritor de log sem `reconfigure`, o que levanta `AttributeError` no import, e a configuração raiz do logging já pertence ao Airflow.

**Files:**
- Modify: `src/pipeline_weather/extract_data.py`
- Modify: `src/pipeline_weather/transform_data.py`
- Modify: `src/pipeline_weather/load_data.py`

**Interfaces:**
- Consumes: nada de tasks anteriores.
- Produces: os três módulos tornam-se importáveis sem efeito colateral. A Task 3 depende disso para importar `extract_data`, `transform_data` e `load_data` dentro das tasks da DAG. Nenhuma assinatura de função muda.

- [ ] **Step 1: Registrar a falha atual**

Este comando simula o que o Airflow faz com o stdout durante uma task: troca `sys.stdout` por um objeto que não expõe `reconfigure`.

```bash
uv run python -c "
import io, sys
class EscritorDeLog(io.TextIOBase):
    def write(self, texto): return len(texto)
sys.stdout = EscritorDeLog()
import pipeline_weather.extract_data
"
```

Esperado: falha com `AttributeError: 'EscritorDeLog' object has no attribute 'reconfigure'`.

- [ ] **Step 2: Corrigir `extract_data.py`**

Remover do corpo do módulo o bloco abaixo, preservando a linha do `logger`:

```python
# O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)
```

O que fica no corpo do módulo:

```python
logger = logging.getLogger(__name__)
```

E `main()` passa a começar configurando a saída, o que antes acontecia no import:

```python
def main() -> None:
    # O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
    # Fica em main() e não no corpo do módulo: importado pelo Airflow, o stdout da task
    # não é um TextIOWrapper e não aceita reconfigure.
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )
    try:
```

Manter o restante de `main()` exatamente como está.

- [ ] **Step 3: Corrigir `transform_data.py`**

A mesma remoção do corpo do módulo, deixando só `logger = logging.getLogger(__name__)`, e o mesmo bloco no topo de `main()`:

```python
def main() -> None:
    # O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
    # Fica em main() e não no corpo do módulo: importado pelo Airflow, o stdout da task
    # não é um TextIOWrapper e não aceita reconfigure.
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )
    try:
        bruto = carregar_json(ARQUIVO_ENTRADA)
```

- [ ] **Step 4: Corrigir `load_data.py`**

A mesma remoção do corpo do módulo, deixando só `logger = logging.getLogger(__name__)`, e o mesmo bloco no topo de `main()`:

```python
def main() -> None:
    # O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
    # Fica em main() e não no corpo do módulo: importado pelo Airflow, o stdout da task
    # não é um TextIOWrapper e não aceita reconfigure.
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )
    try:
        url = carregar_url_banco()
```

- [ ] **Step 5: Verificar que os três módulos importam sob stdout substituído**

```bash
uv run python -c "
import io, sys
class EscritorDeLog(io.TextIOBase):
    def write(self, texto): return len(texto)
sys.stdout = EscritorDeLog()
from pipeline_weather import extract_data, transform_data, load_data
sys.stdout = sys.__stdout__
print('importou os tres modulos sem efeito colateral')
"
```

Esperado: imprime `importou os tres modulos sem efeito colateral`, sem exceção.

- [ ] **Step 6: Verificar que o CLI segue íntegro**

```bash
uv run pipeline-weather --sem-carga
```

Esperado: as linhas de log com acentuação correta (`[1/3] Extração`, `Colunas normalizadas: 32`, `DataFrame com 1 linha(s) e 22 coluna(s)`, `Carga pulada por --sem-carga`), seguidas do DataFrame impresso. Se o log aparecer sem formato ou sem acento, a configuração não foi movida corretamente para `main()`.

- [ ] **Step 7: Verificar que os entrypoints por módulo seguem íntegros**

```bash
uv run python -m pipeline_weather.transform_data
```

Esperado: log formatado com `Transformação concluída: 1 linha(s), 22 coluna(s)` e o DataFrame impresso. Isso confirma que mover a configuração para `main()` não a perdeu nos entrypoints individuais.

- [ ] **Step 8: Commit**

```bash
git add src/pipeline_weather/extract_data.py src/pipeline_weather/transform_data.py src/pipeline_weather/load_data.py
git commit -m "Move a configuracao de log e de stdout para dentro de main()

Importar os modulos deixava de ser inocuo: reconfigure e basicConfig
rodavam no corpo do modulo. Dentro de uma task do Airflow o stdout nao e
um TextIOWrapper e nao aceita reconfigure, e a configuracao raiz do
logging pertence ao Airflow.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Stack Docker do Airflow

**Files:**
- Create: `airflow/Dockerfile`
- Create: `airflow/docker-compose.yaml`
- Create: `airflow/.env`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: os módulos importáveis da Task 1.
- Produces: um stack com `/opt/airflow/projeto/src` no `PYTHONPATH`, `/opt/airflow/dags` recebendo `airflow/dags/`, e `/opt/airflow/projeto/data` com escrita. A Task 3 grava a DAG em `airflow/dags/` e depende desses caminhos.

- [ ] **Step 1: Criar os diretórios que serão montados**

Criá-los antes do primeiro `up` evita que o Docker os crie pertencendo ao root.

```bash
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/config
```

- [ ] **Step 2: Escrever `airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:3.3.2-python3.12

# As cinco dependencias do pyproject.toml do pipeline. O pacote pipeline_weather
# nao e instalado: ele chega por bind mount em /opt/airflow/projeto, para que
# RAIZ_PROJETO (parents[2] do arquivo do modulo) continue resolvendo para a raiz
# do repositorio, de onde saem config/.env, .env.local e data/.
# O constraints do proprio Airflow evita que essa instalacao mude versoes que o
# Airflow fixa.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt" \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    requests \
    python-dotenv
```

- [ ] **Step 3: Escrever `airflow/docker-compose.yaml`**

```yaml
# Derivado do compose oficial do Airflow 3.3.2, com CeleryExecutor trocado por
# LocalExecutor. Isso dispensa redis, airflow-worker e flower. O triggerer
# tambem sai: serve a operadores deferiveis, que esta DAG nao usa.
# restart: "no" em todos: o stack sobe sob demanda e e derrubado depois, entao
# nao deve voltar sozinho quando o Docker Desktop iniciar.
x-airflow-common: &airflow-common
  build: .
  image: pipeline-weather/airflow:3.3.2
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW__API_AUTH__JWT_SECRET:-pipeline_weather_jwt}
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    # O pacote do pipeline e importado daqui, nao do site-packages.
    PYTHONPATH: /opt/airflow/projeto/src
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
    # O repositorio, montado de forma que RAIZ_PROJETO caia em /opt/airflow/projeto.
    - ../src:/opt/airflow/projeto/src:ro
    - ../config/.env:/opt/airflow/projeto/config/.env:ro
    - ../.env.local:/opt/airflow/projeto/.env.local:ro
    - ../data:/opt/airflow/projeto/data
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on: &airflow-common-depends-on
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: "no"

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config
        chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      # Lidos pelo entrypoint da imagem oficial: migram o schema e criam o admin.
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
    user: "0:0"

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: "no"
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: "no"
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type DagProcessorJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: "no"
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  # Nao sobe com `docker compose up`. Serve a comandos pontuais:
  # docker compose --profile debug run --rm airflow-cli airflow dags list
  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    command:
      - bash
      - -c
      - airflow

volumes:
  postgres-db-volume:
```

- [ ] **Step 4: Escrever `airflow/.env`**

```bash
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW__API_AUTH__JWT_SECRET=pipeline_weather_jwt
```

- [ ] **Step 5: Ignorar os artefatos do Airflow no git**

Acrescentar ao final de `.gitignore`:

```gitignore
# Airflow
airflow/logs/
airflow/plugins/
airflow/config/
```

O padrão `.env` já presente no arquivo cobre `airflow/.env`, porque um padrão sem barra casa em qualquer nível. Confirmar com:

```bash
git check-ignore -v airflow/.env
```

Esperado: uma linha apontando a regra `.env` do `.gitignore`.

- [ ] **Step 6: Confirmar que os arquivos montados existem no host**

Bind mount de arquivo único exige que o arquivo exista. Se não existir, o Docker cria um diretório no lugar e a leitura do env falha de forma pouco óbvia.

```bash
ls -la config/.env .env.local
```

Esperado: os dois listados como arquivos regulares. Se faltar `.env.local`, rodar `neon env pull` antes de seguir.

- [ ] **Step 7: Construir a imagem**

```bash
cd airflow && docker compose build
```

Esperado: build conclui e a imagem `pipeline-weather/airflow:3.3.2` é criada. A instalação do pip deve reportar `pandas-3.0.5`.

- [ ] **Step 8: Subir o stack**

```bash
cd airflow && docker compose up -d
```

- [ ] **Step 9: Verificar que os cinco serviços estão de pé**

```bash
cd airflow && docker compose ps
```

Esperado: `postgres`, `airflow-apiserver`, `airflow-scheduler` e `airflow-dag-processor` em `running`, os três do Airflow com estado `healthy` depois de cerca de um minuto. `airflow-init` aparece como `exited (0)`. Nenhum `airflow-worker`, `redis` ou `flower`.

- [ ] **Step 10: Verificar a interface web**

```bash
curl --fail --silent http://localhost:8080/api/v2/monitor/health
```

Esperado: JSON reportando os componentes como saudáveis.

- [ ] **Step 11: Verificar que o pacote é importável dentro do container e que os caminhos resolvem**

Esta é a verificação central da Task 2: prova que o bind mount preserva a resolução de `RAIZ_PROJETO`.

```bash
cd airflow && docker compose exec airflow-scheduler python -c "
from pipeline_weather import extract_data, load_data
print('RAIZ_PROJETO:', extract_data.RAIZ_PROJETO)
print('config/.env existe:', extract_data.CAMINHO_ENV.exists())
print('.env.local existe:', load_data.CAMINHO_ENV_NEON.exists())
print('API_KEY lida:', bool(extract_data.carregar_api_key()))
print('DATABASE_URL lida:', load_data.carregar_url_banco().startswith('postgresql'))
"
```

Esperado, nesta ordem: `RAIZ_PROJETO: /opt/airflow/projeto`, `config/.env existe: True`, `.env.local existe: True`, `API_KEY lida: True`, `DATABASE_URL lida: True`.

- [ ] **Step 12: Verificar que o container alcança a Neon**

```bash
cd airflow && docker compose exec airflow-scheduler python -c "
from sqlalchemy import text
from pipeline_weather import load_data
engine = load_data.criar_engine(load_data.carregar_url_banco())
with engine.connect() as conexao:
    print('banco:', conexao.execute(text('select current_database()')).scalar())
"
```

Esperado: `banco: neondb`. Falha aqui indica container sem saída para a internet ou `.env.local` desatualizado.

- [ ] **Step 13: Commit**

```bash
git add airflow/Dockerfile airflow/docker-compose.yaml .gitignore
git commit -m "Adiciona o stack do Airflow em Docker

Compose derivado do oficial 3.3.2, com LocalExecutor no lugar do Celery:
cinco servicos ativos em vez de dez. O repositorio entra por bind mount em
/opt/airflow/projeto, o que preserva a resolucao de RAIZ_PROJETO e mantem
os segredos fora da imagem.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

`airflow/.env` fica fora do commit por estar ignorado.

---

### Task 3: A DAG clima_teresina

**Files:**
- Create: `airflow/dags/dag_clima_teresina.py`

**Interfaces:**
- Consumes: `extract_data.CIDADE`, `extract_data.carregar_api_key()`, `extract_data.extrair_clima(cidade: str, api_key: str) -> dict`, `extract_data.salvar_json(dados: dict, caminho: Path) -> None`, `transform_data.carregar_json(caminho: Path) -> dict`, `transform_data.transformar(bruto: dict) -> pd.DataFrame`, `load_data.carregar_url_banco() -> str`, `load_data.criar_engine(url: str) -> Engine`, `load_data.criar_tabela(engine: Engine) -> None`, `load_data.carregar(df: pd.DataFrame, engine: Engine) -> int`. Nenhuma dessas assinaturas muda neste plano.
- Produces: a DAG `clima_teresina` com as tasks `extrair`, `transformar` e `carregar`, nesta ordem.

- [ ] **Step 1: Escrever `airflow/dags/dag_clima_teresina.py`**

```python
"""Orquestra o pipeline do clima de Teresina: extracao, transformacao e carga.

As tres tasks chamam as funcoes que o pacote pipeline_weather ja expoe. O dado
trafega por arquivo em data/execucoes/<run_id>/, e o XCom carrega apenas os
caminhos e a contagem final.
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, task

from pipeline_weather import extract_data, load_data, transform_data

FUSO = pendulum.timezone("America/Fortaleza")
RAIZ_EXECUCOES = Path("/opt/airflow/projeto/data/execucoes")

# Colunas de data do DataFrame. pd.read_json nao as reconverte sozinho: a
# heuristica de datas dele olha nomes como 'date' ou sufixo '_at', que nenhuma
# delas tem. Sem a reconversao, chegariam strings onde _valor_python espera
# pd.Timestamp, e observado_em compoe a chave primaria de clima_atual.
COLUNAS_DATA = ["observado_em", "extraido_em", "nascer_do_sol", "por_do_sol"]


def diretorio_da_execucao(run_id: str) -> Path:
    """Converte o run_id em um nome de diretorio valido tambem no Windows.

    O run_id do Airflow tem a forma scheduled__2026-09-17T23:00:00+00:00, com ':'
    e '+'. Como data/ e um bind mount de um disco NTFS, esses caracteres impedem
    a criacao do diretorio.
    """
    return RAIZ_EXECUCOES / re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


@dag(
    dag_id="clima_teresina",
    description="Extrai o clima atual de Teresina, normaliza e carrega no Postgres da Neon",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["clima", "etl", "neon"],
)
def clima_teresina():
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def extrair(**contexto) -> str:
        """Consulta a API e grava o JSON bruto. Tres tentativas: depende de rede."""
        destino = diretorio_da_execucao(contexto["run_id"]) / "bruto.json"
        api_key = extract_data.carregar_api_key()
        bruto = extract_data.extrair_clima(extract_data.CIDADE, api_key)
        extract_data.salvar_json(bruto, destino)
        return str(destino)

    @task(retries=1)
    def transformar(caminho_bruto: str, **contexto) -> str:
        """Normaliza o JSON no DataFrame e grava o intermediario.

        Uma tentativa so: aqui a falha vem de dado malformado, que retry nao
        conserta.
        """
        bruto = transform_data.carregar_json(Path(caminho_bruto))
        df = transform_data.transformar(bruto)

        destino = diretorio_da_execucao(contexto["run_id"]) / "transformado.json"
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_text(
            df.to_json(orient="records", date_format="iso"), encoding="utf-8"
        )
        return str(destino)

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def carregar(caminho_transformado: str) -> int:
        """Insere o DataFrame em clima_atual e devolve as linhas gravadas.

        Tres tentativas: o compute da Neon escala a zero e a primeira conexao
        pode expirar.
        """
        df = pd.read_json(Path(caminho_transformado), orient="records")
        for coluna in COLUNAS_DATA:
            df[coluna] = pd.to_datetime(df[coluna], format="ISO8601", utc=True)

        engine = load_data.criar_engine(load_data.carregar_url_banco())
        load_data.criar_tabela(engine)
        inseridas = load_data.carregar(df, engine)
        print(f"{inseridas} linha(s) inserida(s) em {load_data.TABELA}")
        return inseridas

    carregar(transformar(extrair()))


clima_teresina()
```

- [ ] **Step 2: Verificar que a DAG carrega sem erro de import**

```bash
cd airflow && docker compose --profile debug run --rm airflow-cli airflow dags list-import-errors
```

Esperado: `No data found` ou lista vazia. Qualquer traceback aqui é erro de import da DAG e deve ser corrigido antes de seguir.

- [ ] **Step 3: Verificar que a DAG aparece com as três tasks**

```bash
cd airflow && docker compose --profile debug run --rm airflow-cli airflow tasks list clima_teresina
```

Esperado: `extrair`, `transformar` e `carregar`.

- [ ] **Step 4: Registrar a contagem no banco antes da execução**

```bash
uv run python -c "
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv(Path('.env.local'), override=True)
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conexao:
    print('antes:', conexao.execute(text('select count(*) from clima_atual')).scalar())
"
```

Anotar o número.

- [ ] **Step 5: Executar a DAG de ponta a ponta**

```bash
cd airflow && docker compose exec airflow-scheduler airflow dags test clima_teresina
```

Esperado: as três tasks com estado `success`, e nos logs as mensagens que o pacote já emite (`Consultando clima atual de Teresina,PI,BR`, `Colunas normalizadas: 32`, `Tabela clima_atual disponível`), agora com nível de log preservado pelos handlers do Airflow.

- [ ] **Step 6: Verificar os arquivos intermediários**

```bash
ls data/execucoes/*/
```

Esperado: `bruto.json` e `transformado.json` dentro de um diretório cujo nome é o `run_id` sanitizado, sem `:` nem `+`.

- [ ] **Step 7: Verificar o efeito no banco**

```bash
uv run python -c "
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
load_dotenv(Path('.env.local'), override=True)
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conexao:
    print('depois:', conexao.execute(text('select count(*) from clima_atual')).scalar())
    print(conexao.execute(text(
        'select cidade, observado_em, temperatura_c from clima_atual order by observado_em desc limit 3'
    )).fetchall())
"
```

Esperado: a contagem subiu em 1 em relação ao Step 4, ou permaneceu igual com a task de carga reportando zero inseridas. As duas saídas são corretas: a carga é idempotente por `(id_cidade, observado_em)` e a OpenWeatherMap só publica observação nova a cada dez minutos ou mais. Contagem menor, ou erro de tipo em `observado_em`, indica que a reconversão de datas do Step 1 falhou.

- [ ] **Step 8: Commit**

```bash
git add airflow/dags/dag_clima_teresina.py
git commit -m "Adiciona a DAG clima_teresina

Tres tasks em TaskFlow chamando as funcoes do pacote, com retry por etapa:
a extracao depende da OpenWeatherMap e a carga depende do compute da Neon,
que escala a zero. O dado passa por arquivo em data/execucoes/<run_id>/, com
o run_id sanitizado porque data/ e um bind mount de disco NTFS.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Documentar a orquestração no README

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: o stack da Task 2 e a DAG da Task 3, ambos verificados.
- Produces: nada que outra task consuma.

- [ ] **Step 1: Ler o README para casar o tom e a estrutura**

```bash
cat README.md
```

Identificar o nível de cabeçalho usado nas seções existentes e onde a nova seção se encaixa, depois da execução manual do pipeline.

- [ ] **Step 2: Inserir a seção de orquestração**

Inserir entre o fim da seção `## Uso` (depois do bloco dos três `python -m`) e o início de
`## Transformação`, com este texto:

````markdown
## Orquestração com Airflow

O pipeline também roda como uma DAG do Apache Airflow, em containers, sem depender do `uv`
instalado na máquina. O stack fica em `airflow/` e parte da imagem oficial
`apache/airflow:3.3.2`.

Pré-requisitos: Docker em execução, `config/.env` com a `API_KEY` e `.env.local` com a
`DATABASE_URL`, os mesmos arquivos que a execução local já usa. Eles entram nos containers
montados somente leitura, então nenhum segredo vai para a imagem.

```bash
cd airflow
docker compose up -d
```

A interface fica em http://localhost:8080, com usuário e senha `airflow`. A DAG
`clima_teresina` nasce pausada, como é o padrão do Airflow: despause na interface para o
agendamento horário passar a valer.

Para uma execução avulsa, sem esperar o agendamento nem despausar:

```bash
docker compose exec airflow-scheduler airflow dags test clima_teresina
```

Para derrubar:

```bash
docker compose down        # preserva o histórico das execuções
docker compose down -v     # descarta também o metadata DB do Airflow
```

| Task | Papel | Tentativas |
|---|---|---|
| `extrair` | Consulta a API e grava o JSON bruto | 3, porque depende de rede |
| `transformar` | Normaliza o JSON e grava o intermediário | 1, porque falha aqui é dado malformado |
| `carregar` | Insere em `clima_atual` | 3, porque o compute do Neon hiberna |

O dado passa de uma task para a seguinte por arquivo, em `data/execucoes/<run_id>/`, um
diretório por execução. Isso mantém cada execução inspecionável depois do fato e impede que
uma nova tentativa da carga leia o intermediário de outra execução.

O Postgres que sobe no compose guarda apenas o metadata do Airflow. Os dados do clima
continuam indo para o Neon, então os containers precisam de saída para a internet.

Como a carga é idempotente, uma execução poucos minutos depois de outra reporta zero linhas
inseridas. É o comportamento esperado, não falha: a API só publica leitura nova a cada dez
minutos.
````

- [ ] **Step 3: Atualizar a árvore da seção `## Estrutura`**

Acrescentar as entradas novas, mantendo os comentários alinhados como estão no arquivo:

````markdown
```
pipeline_weather/
├── airflow/
│   ├── dags/
│   │   └── dag_clima_teresina.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── config/
│   ├── .env              # API_KEY e, opcionalmente, DATABASE_URL (ignorado pelo git)
│   └── .env.exemple      # Modelo com placeholders
├── data/                 # Saída da extração e das execuções da DAG (ignorado pelo git)
│   └── weather_piaui.json
├── docs/superpowers/     # Spec de design e plano de implementação
├── src/pipeline_weather/
│   ├── main.py           # Orquestrador e CLI
│   ├── extract_data.py
│   ├── transform_data.py
│   └── load_data.py
├── neon.ts               # Política de branch do Neon
└── pyproject.toml
```
````

- [ ] **Step 4: Verificar os comandos documentados**

Executar cada comando citado na seção nova, na ordem em que aparece, confirmando que produz o resultado descrito. Corrigir o texto onde divergir.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "Documenta a orquestracao com Airflow no README

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Notas de execução

O stack roda a partir de `airflow/`, então os comandos de `docker compose` pedem `cd airflow` e os de `uv` pedem a raiz do repositório. Os passos acima já indicam qual é qual.

Se o `airflow-init` falhar com erro de permissão em `/opt/airflow/logs`, o diretório foi criado pelo Docker pertencendo ao root antes do Step 1 da Task 2. A correção é `docker compose down`, apagar `airflow/logs`, recriar com `mkdir` e subir de novo.
