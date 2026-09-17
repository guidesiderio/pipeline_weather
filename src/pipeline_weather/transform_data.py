"""Transforma o JSON bruto do clima em um DataFrame tabular pronto para carga."""

import json
import logging
import sys
from datetime import timedelta, timezone
from pathlib import Path

import pandas as pd

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
ARQUIVO_ENTRADA = RAIZ_PROJETO / "data" / "weather_piaui.json"

# Colunas da API que não agregam valor analítico (códigos internos e metadados da resposta).
COLUNAS_DESCARTADAS = [
    "base",
    "cod",
    "weather_id",
    "weather_icon",
    "sys_type",
    "sys_id",
    "main_sea_level",
    "main_grnd_level",
    "timezone",
    "cidade_consultada",
]

RENOMEAR_COLUNAS = {
    "name": "cidade",
    "id": "id_cidade",
    "sys_country": "pais",
    "coord_lat": "latitude",
    "coord_lon": "longitude",
    "dt": "observado_em",
    "main_temp": "temperatura_c",
    "main_feels_like": "sensacao_termica_c",
    "main_temp_min": "temperatura_min_c",
    "main_temp_max": "temperatura_max_c",
    "main_humidity": "umidade_pct",
    "main_pressure": "pressao_hpa",
    "wind_speed": "vento_velocidade_ms",
    "wind_deg": "vento_direcao_graus",
    "clouds_all": "nebulosidade_pct",
    "visibility": "visibilidade_m",
    "weather_main": "condicao",
    "weather_description": "descricao",
    "sys_sunrise": "nascer_do_sol",
    "sys_sunset": "por_do_sol",
}

ORDEM_COLUNAS = [
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

COLUNAS_EPOCH = ["observado_em", "nascer_do_sol", "por_do_sol"]

# O console do Windows usa cp1252 por padrão; força UTF-8 para preservar os acentos.
sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def carregar_json(caminho: Path) -> dict:
    """Lê o JSON gerado pela extração. Levanta FileNotFoundError se o arquivo não existir."""
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {caminho}. Rode a extração antes da transformação."
        )
    with caminho.open(encoding="utf-8") as arquivo:
        return json.load(arquivo)


def achatar_registro(bruto: dict) -> dict:
    """Junta os metadados do envelope com a resposta da API em um único dicionário.

    O campo ``weather`` da API é uma lista; só a primeira condição é aproveitada,
    para que ``json_normalize`` consiga achatá-la como um dicionário comum.
    """
    dados = dict(bruto.get("dados", {}))
    condicoes = dados.pop("weather", []) or [{}]
    dados["weather"] = condicoes[0]

    return {
        "extraido_em": bruto.get("extraido_em"),
        "fonte": bruto.get("fonte"),
        "cidade_consultada": bruto.get("cidade"),
        **dados,
    }


def criar_dataframe(registros: list[dict]) -> pd.DataFrame:
    """Normaliza os dicionários aninhados em colunas planas separadas por ``_``."""
    return pd.json_normalize(registros, sep="_")


def converter_timestamps(df: pd.DataFrame, fuso_segundos: int) -> pd.DataFrame:
    """Converte os epochs em UTC da API para o fuso local informado pela própria resposta."""
    fuso_local = timezone(timedelta(seconds=fuso_segundos))
    for coluna in COLUNAS_EPOCH:
        if coluna in df.columns:
            df[coluna] = (
                pd.to_datetime(df[coluna], unit="s", utc=True).dt.tz_convert(fuso_local)
            )
    if "extraido_em" in df.columns:
        df["extraido_em"] = pd.to_datetime(df["extraido_em"], format="ISO8601")
    return df


def transformar(bruto: dict) -> pd.DataFrame:
    """Aplica a transformação completa: achata, normaliza, descarta, renomeia e ordena."""
    df = criar_dataframe([achatar_registro(bruto)])
    logger.info("Colunas normalizadas: %d", df.shape[1])

    fuso_segundos = int(df["timezone"].iloc[0]) if "timezone" in df.columns else 0

    df = df.drop(columns=COLUNAS_DESCARTADAS, errors="ignore")
    df = df.rename(columns=RENOMEAR_COLUNAS)
    df = converter_timestamps(df, fuso_segundos)

    colunas_finais = [coluna for coluna in ORDEM_COLUNAS if coluna in df.columns]
    restantes = [coluna for coluna in df.columns if coluna not in colunas_finais]
    if restantes:
        logger.warning("Colunas fora do mapeamento mantidas ao final: %s", restantes)

    return df[colunas_finais + restantes]


def main() -> None:
    try:
        bruto = carregar_json(ARQUIVO_ENTRADA)
    except (FileNotFoundError, json.JSONDecodeError) as erro:
        logger.error("%s", erro)
        sys.exit(1)

    df = transformar(bruto)

    logger.info("Transformação concluída: %d linha(s), %d coluna(s)", *df.shape)
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
