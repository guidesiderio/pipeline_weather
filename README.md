# pipeline_weather

Pipeline ETL que coleta o clima atual de Teresina-PI na API da OpenWeatherMap, normaliza a
resposta em formato tabular e carrega o resultado em um Postgres gerenciado no Neon.

## Arquitetura

O pipeline tem três etapas, cada uma em um módulo próprio e executável de forma isolada:

| Etapa | Módulo | Entrada | Saída |
|---|---|---|---|
| Extração | `extract_data.py` | API da OpenWeatherMap | `data/weather_piaui.json` |
| Transformação | `transform_data.py` | JSON bruto | `DataFrame` de 22 colunas |
| Carga | `load_data.py` | `DataFrame` | Tabela `clima_atual` no Postgres |

O `main.py` orquestra as três, importando as funções de cada módulo em vez de chamar seus
`main()`, que encerram o processo com `sys.exit()`.

A extração envelopa a resposta da API com metadados de linhagem (`extraido_em`, `fonte`,
`cidade`) antes de gravar o JSON, o que preserva o momento da coleta mesmo que o arquivo seja
reprocessado depois.

## Requisitos

- Python 3.12 ou superior
- [uv](https://docs.astral.sh/uv/) para gerenciar o ambiente
- Uma API key da [OpenWeatherMap](https://openweathermap.org/api) (o plano gratuito atende)
- Um banco Postgres acessível (o projeto usa o [Neon](https://neon.com))

Dependências Python: `pandas`, `requests`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`.

## Configuração

### 1. Instalar as dependências

```bash
uv sync
```

### 2. Definir a API key

Copie `config/.env.exemple` para `config/.env` e preencha a chave:

```
API_KEY='sua_chave_da_openweathermap'
```

### 3. Definir a conexão com o banco

Há dois caminhos. Com a CLI do Neon, o vínculo do diretório gera o arquivo automaticamente:

```bash
neon link --project-id <id-do-projeto> --branch production
```

Isso grava `DATABASE_URL`, `DATABASE_URL_UNPOOLED` e `NEON_BRANCH` em `.env.local` na raiz,
e reescreve esse arquivo a cada `neon link` ou `neon deploy`.

Sem a CLI, defina `DATABASE_URL` manualmente em `config/.env`:

```
DATABASE_URL='postgresql://usuario:senha@host/banco?sslmode=require'
```

O `load_data.py` lê os dois arquivos, com `.env.local` tendo precedência. O prefixo
`postgresql://` é convertido para `postgresql+psycopg2://` em tempo de execução, então a
string pode ser colada do painel do Neon sem ajustes.

`config/.env`, `.env.local` e `.neon` estão no `.gitignore` e não devem ser versionados.

## Uso

Pipeline completo:

```bash
uv run pipeline-weather
```

| Flag | Efeito |
|---|---|
| `--cidade CIDADE` | Cidade consultada na API. Padrão: `Teresina,PI,BR` |
| `--sem-extracao` | Pula a chamada à API e reaproveita o JSON já salvo |
| `--sem-carga` | Para após a transformação e imprime o `DataFrame`, sem tocar no banco |

As flags existem porque cada etapa tem um custo diferente. Para depurar a transformação sem
gastar uma chamada da API nem escrever no banco:

```bash
uv run pipeline-weather --sem-extracao --sem-carga
```

O diretório `data/` não é versionado, então em um clone novo a primeira execução precisa
incluir a extração. O `--sem-extracao` só funciona depois que existir um JSON salvo.

Cada etapa também roda isolada:

```bash
uv run python -m pipeline_weather.extract_data
uv run python -m pipeline_weather.transform_data
uv run python -m pipeline_weather.load_data
```

## Transformação

A resposta da API é um JSON aninhado. A transformação aplica quatro operações:

1. Achatamento dos dicionários aninhados com `pd.json_normalize`, usando `_` como separador.
   O campo `weather` é uma lista, e só a primeira condição é aproveitada.
2. Descarte de campos sem valor analítico: `base`, `cod`, `weather_id`, `weather_icon`,
   `sys_type`, `sys_id`, `main_sea_level`, `main_grnd_level`.
3. Renomeação para `snake_case` em português, com a unidade no sufixo (`temperatura_c`,
   `umidade_pct`, `vento_velocidade_ms`).
4. Conversão dos epochs em UTC (`dt`, `sunrise`, `sunset`) para o fuso local, usando o offset
   que a própria resposta informa no campo `timezone`.

## Esquema da tabela

A tabela `clima_atual` é criada na primeira execução pelo `CREATE TABLE IF NOT EXISTS`, sem
passo manual de migração.

| Coluna | Tipo | Origem na API |
|---|---|---|
| `cidade` | `TEXT NOT NULL` | `name` |
| `pais` | `TEXT` | `sys.country` |
| `latitude` / `longitude` | `DOUBLE PRECISION` | `coord.lat` / `coord.lon` |
| `observado_em` | `TIMESTAMPTZ NOT NULL` | `dt` |
| `temperatura_c` | `DOUBLE PRECISION` | `main.temp` |
| `sensacao_termica_c` | `DOUBLE PRECISION` | `main.feels_like` |
| `temperatura_min_c` / `temperatura_max_c` | `DOUBLE PRECISION` | `main.temp_min` / `main.temp_max` |
| `umidade_pct` | `INTEGER` | `main.humidity` |
| `pressao_hpa` | `INTEGER` | `main.pressure` |
| `vento_velocidade_ms` | `DOUBLE PRECISION` | `wind.speed` |
| `vento_direcao_graus` | `INTEGER` | `wind.deg` |
| `nebulosidade_pct` | `INTEGER` | `clouds.all` |
| `visibilidade_m` | `INTEGER` | `visibility` |
| `condicao` / `descricao` | `TEXT` | `weather[0].main` / `weather[0].description` |
| `nascer_do_sol` / `por_do_sol` | `TIMESTAMPTZ` | `sys.sunrise` / `sys.sunset` |
| `id_cidade` | `BIGINT NOT NULL` | `id` |
| `fonte` | `TEXT` | Metadado da extração |
| `extraido_em` | `TIMESTAMPTZ` | Metadado da extração |
| `carregado_em` | `TIMESTAMPTZ` | `now()` no momento da carga |

### Idempotência

A chave primária é composta por `(id_cidade, observado_em)` e o `INSERT` usa
`ON CONFLICT DO NOTHING`. A OpenWeatherMap atualiza a leitura a cada dez minutos, então
reexecutar o pipeline em intervalo menor que esse não duplica a linha. O log informa quantas
linhas entraram e quantas já existiam.

A DDL é declarada explicitamente em vez de inferida pelo `to_sql` do pandas, porque o
`to_sql` não cria chave primária nem usa `TIMESTAMPTZ`, e sem a chave composta o
`ON CONFLICT` não funciona.

## Estrutura

```
pipeline_weather/
├── config/
│   ├── .env              # API_KEY e, opcionalmente, DATABASE_URL (ignorado pelo git)
│   └── .env.exemple      # Modelo com placeholders
├── data/                 # Saída da extração (ignorado pelo git)
│   └── weather_piaui.json
├── src/pipeline_weather/
│   ├── main.py           # Orquestrador e CLI
│   ├── extract_data.py
│   ├── transform_data.py
│   └── load_data.py
├── neon.ts               # Política de branch do Neon
└── pyproject.toml
```

## Solução de problemas

**`API_KEY ausente ou vazia`**

O arquivo `config/.env` não existe ou a variável está em branco. Copie o `.env.exemple` e
preencha a chave.

**`DATABASE_URL ausente ou vazia`**

Rode `neon link` para gerar o `.env.local`, ou defina a variável em `config/.env`.

**`server closed the connection unexpectedly` na etapa de carga**

O TCP na porta 5432 é aceito, mas o servidor não responde ao `SSLRequest` do protocolo
Postgres. Em redes corporativas com inspeção de pacote, o protocolo pode ser descartado
mesmo com a porta aberta e as credenciais válidas.

Para distinguir esse caso de um problema real de credencial ou de banco, consulte o mesmo
host pela porta 443, que o filtro costuma liberar:

```bash
curl -s -X POST "https://$HOST/sql" \
  -H "Content-Type: application/json" \
  -H "Neon-Connection-String: $DATABASE_URL" \
  -d '{"query":"select version()","params":[]}'
```

Se a query retornar normalmente, o banco e as credenciais estão corretos e o bloqueio é da
rede. As alternativas são rodar o pipeline em outra rede ou adaptar a carga para o endpoint
SQL-over-HTTP do Neon, na porta 443.

**Compute do Neon suspenso**

No plano gratuito o compute hiberna após alguns minutos de inatividade. A engine é criada
com `pool_pre_ping=True`, que descarta conexões mortas do pool e reabre, então a primeira
execução após a hibernação pode demorar alguns segundos a mais em vez de falhar.
