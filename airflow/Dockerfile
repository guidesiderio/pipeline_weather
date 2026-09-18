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
