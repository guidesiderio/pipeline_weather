"""Carrega o DataFrame transformado em uma tabela Postgres gerenciada (Neon)."""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from pipeline_weather.transform_data import ARQUIVO_ENTRADA, carregar_json, transformar

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
CAMINHO_ENV = RAIZ_PROJETO / "config" / ".env"
# O `neon link` regrava o DATABASE_URL aqui a cada deploy, então ele tem precedência.
CAMINHO_ENV_NEON = RAIZ_PROJETO / ".env.local"

TABELA = "clima_atual"

# A chave primária composta deixa a carga idempotente: reexecutar o pipeline sobre a
# mesma leitura da API não duplica a linha, porque o INSERT usa ON CONFLICT DO NOTHING.
CRIAR_TABELA = f"""
CREATE TABLE IF NOT EXISTS {TABELA} (
    cidade               TEXT NOT NULL,
    pais                 TEXT,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    observado_em         TIMESTAMPTZ NOT NULL,
    temperatura_c        DOUBLE PRECISION,
    sensacao_termica_c   DOUBLE PRECISION,
    temperatura_min_c    DOUBLE PRECISION,
    temperatura_max_c    DOUBLE PRECISION,
    umidade_pct          INTEGER,
    pressao_hpa          INTEGER,
    vento_velocidade_ms  DOUBLE PRECISION,
    vento_direcao_graus  INTEGER,
    nebulosidade_pct     INTEGER,
    visibilidade_m       INTEGER,
    condicao             TEXT,
    descricao            TEXT,
    nascer_do_sol        TIMESTAMPTZ,
    por_do_sol           TIMESTAMPTZ,
    id_cidade            BIGINT NOT NULL,
    fonte                TEXT,
    extraido_em          TIMESTAMPTZ,
    carregado_em         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id_cidade, observado_em)
)
"""

COLUNAS = [
    "cidade",
    "pais",
    "latitude",
    "longitude",
    "observado_em",
    "temperatura_c",
    "sensacao_termica_c",
    "temperatura_min_c",
    "temperatura_max_c",
    "umidade_pct",
    "pressao_hpa",
    "vento_velocidade_ms",
    "vento_direcao_graus",
    "nebulosidade_pct",
    "visibilidade_m",
    "condicao",
    "descricao",
    "nascer_do_sol",
    "por_do_sol",
    "id_cidade",
    "fonte",
    "extraido_em",
]

INSERIR = text(
    f"INSERT INTO {TABELA} ({', '.join(COLUNAS)}) "
    f"VALUES ({', '.join(f':{coluna}' for coluna in COLUNAS)}) "
    "ON CONFLICT (id_cidade, observado_em) DO NOTHING"
)

logger = logging.getLogger(__name__)


def carregar_url_banco() -> str:
    """Lê DATABASE_URL do config/.env. Levanta RuntimeError se ausente ou vazia."""
    load_dotenv(CAMINHO_ENV)
    load_dotenv(CAMINHO_ENV_NEON, override=True)
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError(
            f"DATABASE_URL ausente ou vazia. Rode `neon link` para gerar "
            f"{CAMINHO_ENV_NEON} ou defina-a em {CAMINHO_ENV}"
        )
    # A URL copiada do painel do Neon vem no formato genérico; o SQLAlchemy precisa do driver.
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        logger.warning("DATABASE_URL sem sslmode; o Neon exige sslmode=require")
    return url


def criar_engine(url: str) -> Engine:
    """Cria a engine com pre-ping, que reabre a conexão se o compute do Neon hibernou."""
    return create_engine(url, pool_pre_ping=True)


def criar_tabela(engine: Engine) -> None:
    """Garante a existência da tabela de destino."""
    with engine.begin() as conexao:
        conexao.execute(text(CRIAR_TABELA))
    logger.info("Tabela %s disponível", TABELA)


def _valor_python(valor):
    """Converte escalares do pandas/numpy em tipos nativos que o psycopg2 aceita."""
    if isinstance(valor, pd.Timestamp):
        return None if pd.isna(valor) else valor.to_pydatetime()
    if valor is None or pd.isna(valor):
        return None
    if hasattr(valor, "item"):
        return valor.item()
    return valor


def montar_registros(df: pd.DataFrame) -> list[dict]:
    """Converte o DataFrame na lista de parâmetros nomeados usada pelo INSERT."""
    faltantes = [coluna for coluna in COLUNAS if coluna not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no DataFrame: {faltantes}")

    return [
        {coluna: _valor_python(linha[coluna]) for coluna in COLUNAS}
        for _, linha in df.iterrows()
    ]


def carregar(df: pd.DataFrame, engine: Engine) -> int:
    """Insere os registros e devolve quantas linhas foram efetivamente gravadas."""
    registros = montar_registros(df)
    with engine.begin() as conexao:
        resultado = conexao.execute(INSERIR, registros)
    return resultado.rowcount


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
        bruto = carregar_json(ARQUIVO_ENTRADA)
    except (RuntimeError, FileNotFoundError) as erro:
        logger.error("%s", erro)
        sys.exit(1)

    df = transformar(bruto)

    try:
        engine = criar_engine(url)
        criar_tabela(engine)
        inseridas = carregar(df, engine)
    except SQLAlchemyError as erro:
        logger.error("Falha na carga: %s", erro)
        sys.exit(1)

    ignoradas = len(df) - inseridas
    logger.info(
        "Carga concluída em %s: %d inserida(s), %d já existente(s)",
        TABELA,
        inseridas,
        ignoradas,
    )


if __name__ == "__main__":
    main()
