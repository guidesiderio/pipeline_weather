"""Extrai o clima atual de Teresina-PI da OpenWeatherMap e salva em data/."""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

RAIZ_PROJETO = Path(__file__).resolve().parents[2]
CAMINHO_ENV = RAIZ_PROJETO / "config" / ".env"
ARQUIVO_SAIDA = RAIZ_PROJETO / "data" / "weather_piaui.json"

URL_API = "https://api.openweathermap.org/data/2.5/weather"
CIDADE = "Teresina,PI,BR"
TIMEOUT_SEGUNDOS = 10

logger = logging.getLogger(__name__)


def carregar_api_key() -> str:
    """Lê API_KEY do config/.env. Levanta RuntimeError se ausente ou vazia."""
    load_dotenv(CAMINHO_ENV)
    api_key = (os.getenv("API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            f"API_KEY ausente ou vazia. Defina-a em {CAMINHO_ENV}"
        )
    return api_key


def extrair_clima(cidade: str, api_key: str) -> dict:
    """Consulta o clima atual da cidade e devolve a resposta envelopada com metadados."""
    logger.info("Consultando clima atual de %s", cidade)
    resposta = requests.get(
        URL_API,
        params={
            "q": cidade,
            "appid": api_key,
            "units": "metric",
            "lang": "pt_br",
        },
        timeout=TIMEOUT_SEGUNDOS,
    )
    resposta.raise_for_status()

    return {
        "extraido_em": datetime.now().astimezone().isoformat(),
        "fonte": "openweathermap/weather",
        "cidade": cidade,
        "dados": resposta.json(),
    }


def salvar_json(dados: dict, caminho: Path) -> None:
    """Grava os dados como JSON UTF-8, criando o diretório de destino se preciso."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, indent=2, ensure_ascii=False)
    logger.info("Dados salvos em %s", caminho)


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
        api_key = carregar_api_key()
        dados = extrair_clima(CIDADE, api_key)
        salvar_json(dados, ARQUIVO_SAIDA)
    except RuntimeError as erro:
        logger.error("%s", erro)
        sys.exit(1)
    except requests.RequestException as erro:
        logger.error("Falha ao consultar a API: %s", erro)
        sys.exit(1)

    temperatura = dados["dados"]["main"]["temp"]
    descricao = dados["dados"]["weather"][0]["description"]
    logger.info("Extração concluída: %.1f °C, %s", temperatura, descricao)


if __name__ == "__main__":
    main()
