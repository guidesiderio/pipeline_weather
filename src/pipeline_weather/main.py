"""Orquestra o pipeline completo: extração, transformação e carga."""

import argparse
import logging
import sys

import pandas as pd
import requests
from sqlalchemy.exc import SQLAlchemyError

from pipeline_weather.extract_data import (
    ARQUIVO_SAIDA,
    CIDADE,
    carregar_api_key,
    extrair_clima,
    salvar_json,
)
from pipeline_weather.load_data import (
    TABELA,
    carregar,
    carregar_url_banco,
    criar_engine,
    criar_tabela,
)
from pipeline_weather.transform_data import carregar_json, transformar

# O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def extrair(cidade: str) -> dict:
    """Consulta a API e salva o JSON bruto em data/."""
    logger.info("[1/3] Extração")
    api_key = carregar_api_key()
    bruto = extrair_clima(cidade, api_key)
    salvar_json(bruto, ARQUIVO_SAIDA)
    return bruto


def transformar_etapa(bruto: dict) -> pd.DataFrame:
    """Normaliza o JSON bruto no DataFrame tabular."""
    logger.info("[2/3] Transformação")
    df = transformar(bruto)
    logger.info("DataFrame com %d linha(s) e %d coluna(s)", *df.shape)
    return df


def carregar_etapa(df: pd.DataFrame) -> int:
    """Grava o DataFrame na tabela de destino e devolve as linhas inseridas."""
    logger.info("[3/3] Carga")
    engine = criar_engine(carregar_url_banco())
    criar_tabela(engine)
    return carregar(df, engine)


def montar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-weather",
        description="Extrai o clima atual da OpenWeatherMap, normaliza e carrega no Postgres.",
    )
    parser.add_argument(
        "--cidade",
        default=CIDADE,
        help=f"cidade consultada na API (padrão: {CIDADE})",
    )
    parser.add_argument(
        "--sem-extracao",
        action="store_true",
        help=f"pula a chamada à API e reaproveita o JSON já salvo em {ARQUIVO_SAIDA.name}",
    )
    parser.add_argument(
        "--sem-carga",
        action="store_true",
        help="para após a transformação e imprime o DataFrame, sem tocar no banco",
    )
    return parser


def main() -> None:
    args = montar_parser().parse_args()

    try:
        bruto = carregar_json(ARQUIVO_SAIDA) if args.sem_extracao else extrair(args.cidade)
        df = transformar_etapa(bruto)

        if args.sem_carga:
            logger.info("Carga pulada por --sem-carga")
            with pd.option_context("display.max_columns", None, "display.width", 200):
                print(df.to_string(index=False))
            return

        inseridas = carregar_etapa(df)
    except RuntimeError as erro:
        logger.error("%s", erro)
        sys.exit(1)
    except FileNotFoundError as erro:
        logger.error("%s", erro)
        sys.exit(1)
    except requests.RequestException as erro:
        logger.error("Falha ao consultar a API: %s", erro)
        sys.exit(1)
    except SQLAlchemyError as erro:
        logger.error("Falha na carga: %s", erro)
        sys.exit(1)

    ignoradas = len(df) - inseridas
    logger.info(
        "Pipeline concluído em %s: %d inserida(s), %d já existente(s)",
        TABELA,
        inseridas,
        ignoradas,
    )


if __name__ == "__main__":
    main()
