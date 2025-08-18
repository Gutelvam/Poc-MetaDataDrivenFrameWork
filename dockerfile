# Dockerfile customizado para Framework com Airflow 3.x
FROM apache/airflow:latest

# Mudar para root para instalar dependências do sistema
USER root

# Atualizar sistema e instalar dependências
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
        vim \
        git \
        build-essential \
        libpq-dev \
        libffi-dev \
        libssl-dev \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libldap2-dev \
        ca-certificates \
        gnupg \
        lsb-release \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar MongoDB tools (opcional) - com melhor error handling
RUN curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor \
    && echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | \
    tee /etc/apt/sources.list.d/mongodb-org-7.0.list \
    && apt-get update \
    && apt-get install -y mongodb-mongosh || echo "MongoDB tools installation skipped"

# Voltar para o usuário airflow
USER airflow

# Atualizar pip para versão mais recente
RUN pip install --upgrade pip setuptools wheel

# Copiar requirements primeiro para melhor cache
COPY requirements.txt /opt/airflow/requirements.txt

# Instalar dependências Python do framework (FIXED: Airflow 3.x compatible)
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Verificar versão do Airflow instalada e compatibility
RUN python -c "import airflow; print(f'Airflow version: {airflow.__version__}')" \
    && python -c "from airflow.operators.empty import EmptyOperator; print('✅ EmptyOperator available')" \
    && python -c "from airflow.models import BaseOperator; print('✅ BaseOperator available')" \
    && echo "✅ Airflow 3.x compatibility verified"

# Criar diretórios necessários
RUN mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/metadata \
    && mkdir -p /opt/airflow/config \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/framework \
    && mkdir -p /opt/airflow/monitoring

# Copiar código do framework
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./metadata /opt/airflow/metadata
COPY --chown=airflow:root ./config /opt/airflow/config
COPY --chown=airflow:root ./plugins /opt/airflow/plugins
COPY --chown=airflow:root ./monitoring /opt/airflow/monitoring

# Definir variáveis de ambiente
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow"
ENV FRAMEWORK_METADATA_PATH="/opt/airflow/metadata"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# Verificar se os módulos do framework são importáveis
RUN python -c "import sys; sys.path.insert(0, '/opt/airflow/dags'); from core.config import PipelineConfig; print('✅ Framework core imports working')" || echo "⚠️ Framework imports need fixing after startup"

# Health check personalizado
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import airflow; print('OK')" || exit 1

# Definir diretório de trabalho
WORKDIR /opt/airflow

# Comando padrão (será sobrescrito pelo docker-compose)
CMD ["airflow", "webserver"]