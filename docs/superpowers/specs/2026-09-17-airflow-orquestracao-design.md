# Orquestração do pipeline_weather com Apache Airflow

Data: 2026-09-17
Status: aprovado, aguardando plano de implementação

## Contexto

O pipeline extrai o clima atual de Teresina da OpenWeatherMap, normaliza o JSON em um DataFrame de 22 colunas e carrega em `clima_atual`, tabela hospedada em Lakebase Postgres pela Neon. Hoje a execução é manual, pelo console script `pipeline-weather`, que chama as três etapas em sequência dentro de um único processo.

O objetivo é orquestrar essas etapas no Apache Airflow, rodando em Docker a partir da imagem oficial, com uma DAG que torne as três etapas visíveis e reexecutáveis de forma independente.

Ambiente confirmado em 2026-09-17: Docker 29.8.0, Docker Compose v5.5.1, containers Linux, 8,3 GB de memória disponíveis ao daemon. Última versão estável da imagem oficial no Docker Hub: `apache/airflow:3.3.2`.

## Restrições que vêm do código atual

Três características do repositório condicionam o desenho e foram verificadas no código, não presumidas.

A carga é idempotente. O INSERT usa `ON CONFLICT (id_cidade, observado_em) DO NOTHING`, e `(id_cidade, observado_em)` é a chave primária de `clima_atual`. Reexecutar uma etapa ou uma DAG inteira não duplica linha.

Os caminhos de configuração e de dados são derivados da posição do arquivo do módulo: `RAIZ_PROJETO = Path(__file__).resolve().parents[2]`. Disso saem `config/.env`, `.env.local` e `data/`. Instalar o pacote dentro da imagem faria esses três caminhos apontarem para dentro do site-packages do container, onde não existem. Montar o repositório preserva a resolução.

`extract_data.py`, `transform_data.py` e `load_data.py` executam `sys.stdout.reconfigure(encoding="utf-8")` e `logging.basicConfig(...)` no corpo do módulo, ou seja, ao serem importados. Dentro de uma task do Airflow o stdout já foi substituído por um escritor de log que não expõe `reconfigure`, e a configuração raiz do logging já pertence ao Airflow.

## Decisões

### Perfil de uso

Stack de estudo e portfólio, levantado sob demanda e derrubado depois. Isso justifica LocalExecutor no lugar do CeleryExecutor e dispensa política de restart automático.

### Stack Docker

O compose oficial do Airflow 3.3.2 declara dez serviços com CeleryExecutor: `postgres`, `redis`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`, `airflow-init`, `airflow-cli` e `flower`. A versão adotada aqui usa LocalExecutor e cinco serviços ativos:

| Serviço | Papel |
| --- | --- |
| `postgres:16` | metadata DB do Airflow, em volume nomeado |
| `airflow-init` | migração do schema e criação do usuário admin |
| `airflow-apiserver` | interface web e API, publicada em 8080 |
| `airflow-scheduler` | agendamento e execução das tasks |
| `airflow-dag-processor` | parsing das DAGs, componente separado obrigatório no Airflow 3 |

Ficam de fora `redis`, `airflow-worker` e `flower`, que existem apenas para o CeleryExecutor, e `airflow-triggerer`, que serve a operadores deferíveis que esta DAG não usa.

Além dos cinco, o serviço `airflow-cli` é declarado sob um profile do compose, como no arquivo oficial. Ele não sobe com `docker compose up` e existe para comandos pontuais, entre eles a checagem de import da seção de verificação.

`AIRFLOW__CORE__LOAD_EXAMPLES` vai como `false`, para a interface listar somente a DAG do projeto. A autenticação segue o padrão do compose oficial, com FabAuthManager e usuário admin criado pelo `airflow-init`.

### Imagem

`airflow/Dockerfile` parte de `apache/airflow:3.3.2-python3.12` e instala as cinco dependências declaradas no `pyproject.toml`: `pandas`, `sqlalchemy`, `psycopg2-binary`, `requests`, `python-dotenv`. A instalação usa o arquivo de constraints publicado pelo projeto para esta combinação de versões, `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt`, para que a resolução dessas cinco dependências não altere as versões que o próprio Airflow fixa. O pacote `pipeline_weather` não é instalado na imagem.

### Estrutura de diretórios

```
pipeline_weather/
  airflow/
    Dockerfile
    docker-compose.yaml
    .env                        variáveis do compose, fora do git
    dags/dag_clima_teresina.py
    logs/  plugins/  config/    do Airflow, fora do git
  src/  config/  data/          inalterados
```

O stack fica em `airflow/` porque o compose oficial monta `./config` em `/opt/airflow/config` e aponta `AIRFLOW_CONFIG` para lá. O repositório já tem um `config/` com a `API_KEY`, e os dois colidiriam se o compose ficasse na raiz.

### Montagens

| Host | Container | Modo |
| --- | --- | --- |
| `../src` | `/opt/airflow/projeto/src` | leitura |
| `../config/.env` | `/opt/airflow/projeto/config/.env` | leitura |
| `../.env.local` | `/opt/airflow/projeto/.env.local` | leitura |
| `../data` | `/opt/airflow/projeto/data` | escrita |

Com `PYTHONPATH=/opt/airflow/projeto/src`, `RAIZ_PROJETO` resolve para `/opt/airflow/projeto` e os três caminhos derivados caem sobre as montagens correspondentes.

Os segredos permanecem nos arquivos que já existem, fora da imagem e fora do git. O `.env.local` continua sendo regravado no host pelo `neon checkout`, e a execução seguinte da DAG enxerga a URL do novo branch sem rebuild da imagem.

A carga vai para a Neon, não para o Postgres local, que serve apenas ao metadata do Airflow. Os containers precisam de saída para a internet.

### A DAG

Arquivo `airflow/dags/dag_clima_teresina.py`, `dag_id` `clima_teresina`, escrita com a API TaskFlow.

| Parâmetro | Valor | Razão |
| --- | --- | --- |
| `schedule` | `@hourly` | a API publica observação nova a cada dez minutos ou mais |
| fuso | `America/Fortaleza` | fuso de Teresina, alinha os horários da interface |
| `catchup` | `False` | a API devolve o clima do instante da chamada; reprocessar janela passada gravaria a leitura atual |
| `max_active_runs` | `1` | evita duas execuções disputando o mesmo destino |
| pausada ao criar | sim | padrão do Airflow, mantido |

As três tasks importam o pacote por módulo (`from pipeline_weather import extract_data, transform_data, load_data`), para que os nomes das tasks não sombreiem os das funções.

`extrair` chama `carregar_api_key()` e `extrair_clima()`, grava o JSON bruto e devolve o caminho. `transformar` lê esse JSON, aplica `transformar()` e grava o DataFrame como JSON. `carregar` lê o intermediário, abre a engine com `carregar_url_banco()`, garante a tabela com `criar_tabela()` e insere, devolvendo a contagem de linhas gravadas.

### Handoff entre tasks

O dado trafega por arquivo em `data/execucoes/<run_id>/`, um diretório por execução. O XCom transporta apenas os caminhos e a contagem final.

O diretório por execução substitui o nome fixo `data/weather_piaui.json` usado pelo CLI. Isso mantém cada execução inspecionável depois do fato e impede que um retry da carga leia o intermediário de outra execução. As funções do pacote já recebem o caminho como argumento, então isso não exige mudança nelas. Os diretórios acumulam em `data/`, que já está no `.gitignore`.

`pd.read_json` não reconverte `observado_em`, `extraido_em`, `nascer_do_sol` e `por_do_sol`, porque esses nomes não batem com a heurística de colunas de data da função. A task de carga reconverte as quatro com `pd.to_datetime(..., format="ISO8601")` antes de chamar `load_data.carregar`. Sem isso chegariam strings onde `_valor_python` espera `pd.Timestamp`, e `observado_em` compõe a chave primária.

### Retries

| Task | Tentativas | Espera | Razão |
| --- | --- | --- | --- |
| `extrair` | 3 | 2 min | depende da OpenWeatherMap, sujeita a falha transitória |
| `transformar` | 1 | não se aplica | falha apenas por dado malformado, que retry não conserta |
| `carregar` | 3 | 2 min | o compute da Neon escala a zero e a primeira conexão pode expirar |

As exceções do pacote (`RuntimeError`, `FileNotFoundError`, `requests.RequestException`, `SQLAlchemyError`) sobem sem tratamento na DAG, que é o que marca a task como falha.

### Mudança no pacote

Em `extract_data.py`, `transform_data.py` e `load_data.py`, `sys.stdout.reconfigure(encoding="utf-8")` e `logging.basicConfig(...)` saem do corpo do módulo e passam para a primeira linha do `main()` de cada um. `logger = logging.getLogger(__name__)` permanece no nível do módulo.

`main.py` não muda: a DAG nunca o importa e ele já configura o logging do CLI.

Os quatro módulos têm `main()` próprio, então `python -m pipeline_weather.transform_data` continua com log formatado.

A mudança evita o `AttributeError` em `sys.stdout.reconfigure` dentro da task e faz os logs que o pacote já emite subirem pelos handlers do Airflow, aparecendo no log da task com o nível preservado.

## Alternativas descartadas

BashOperator chamando o CLI em subprocesso. Imune ao problema de stdout e sem tocar no pacote, mas o CLI não tem como executar somente a carga: os flags existentes são `--sem-extracao` e `--sem-carga`. Três tasks exigiriam criar entradas de linha de comando por etapa, mais código novo do que a correção adotada, e os logs chegariam como texto de subprocesso, sem níveis.

DockerOperator, com o pipeline em imagem própria e um container por etapa. Dá isolamento real entre as dependências do Airflow e as do pipeline, ao custo de montar o socket do Docker no scheduler, instável no Docker Desktop para Windows, e de compartilhar o volume `data/` entre containers irmãos.

Segredos em Airflow Variables e Connection. Mais idiomático como vitrine, porém precisariam ser recadastrados a cada remoção do volume do metadata DB, e a Connection apenas transportaria a URL que o código já sabe ler dos arquivos.

DAG de task única chamando o CLI. Não exigiria código novo, mas o grafo ficaria com um retângulo só e uma falha na carga obrigaria a repetir a chamada à API.

## Verificação

Não existe suíte de testes nem pytest nas dependências do repositório. Em vez de criar infraestrutura de teste não solicitada, a verificação é executada e conferida em quatro pontos.

1. `uv run pipeline-weather --sem-carga` confirma que o CLI segue íntegro após a mudança no pacote, exercitando extração e transformação sem tocar no banco.
2. `docker compose run --rm airflow-cli airflow dags list-import-errors` confirma que a DAG carrega sem erro de import.
3. `airflow dags test clima_teresina` executa as três tasks em processo e mostra os logs.
4. `select count(*) from clima_atual` na Neon, antes e depois, confirma o efeito no banco.

## Riscos conhecidos

A carga é idempotente por `(id_cidade, observado_em)` e a OpenWeatherMap atualiza a observação a cada dez minutos ou mais. Uma execução da DAG poucos minutos depois de outra execução tende a inserir zero linhas e reportar `0 inserida(s), 1 já existente(s)`. É o comportamento correto, não falha.

A montagem de arquivo único (`.env.local`, `config/.env`) exige que o arquivo exista no host antes do `up`. Se não existir, o Docker cria um diretório no lugar, e a leitura do env falha com erro pouco óbvio.

## Fora de escopo

Suíte de testes automatizados. Deploy do Airflow fora da máquina local. Alteração do schema de `clima_atual`. Uso de branches da Neon por execução da DAG. Alertas ou notificação de falha.
