"""Orquestra o pipeline do clima de Teresina: extracao, transformacao e carga.

As tres tasks chamam as funcoes que o pacote pipeline_weather ja expoe. O dado
trafega por arquivo em data/execucoes/<run_id>/, e o XCom carrega apenas os
caminhos e a contagem final.
"""

from __future__ import annotations

import logging
import re
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.sdk import dag, task

from pipeline_weather import extract_data, load_data, transform_data

logger = logging.getLogger(__name__)

FUSO = pendulum.timezone("America/Fortaleza")
RAIZ_EXECUCOES = extract_data.RAIZ_PROJETO / "data" / "execucoes"

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

    Pontos nas extremidades do nome sanitizado sao descartados: sem isso, um
    run_id igual a ".." sobreviveria inteiro e apontaria para fora de
    data/execucoes.
    """
    nome = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id).strip(".")
    return RAIZ_EXECUCOES / nome


@dag(
    dag_id="clima_teresina",
    description="Extrai o clima atual de Teresina, normaliza e carrega no Postgres da Neon",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 9, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    default_args={"retry_delay": timedelta(minutes=2)},
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

    @task(retries=0)
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
        logger.info("%s linha(s) inserida(s) em %s", inseridas, load_data.TABELA)
        return inseridas

    carregar(transformar(extrair()))


clima_teresina()
