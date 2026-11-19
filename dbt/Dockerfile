
FROM python:3.11-slim

WORKDIR /usr/app

ENV PYTHONUNBUFFERED=1

# Instalar Git
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Instalar versiones fijadas de dbt
RUN pip install --no-cache-dir \
    dbt-core==1.12.7 \
    dbt-snowflake==1.12.5

# Incorporar el proyecto dbt a la imagen
COPY dbt_project.yml profiles.yml ./
COPY macros/ ./macros/
COPY models/ ./models/
COPY tests/ ./tests/

CMD ["tail", "-f", "/dev/null"]
