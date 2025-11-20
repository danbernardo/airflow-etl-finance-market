# Usar a imagem oficial do Airflow como base
FROM apache/airflow:2.5.0

# Trocar para o usuário root para instalar dependências do sistema
USER root

# Instalar postgresql-client (ferramentas PostgreSQL como pg_isready, psql, etc)
# Necessário para conectar ao banco de dados e executar health checks
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Voltar para o usuário airflow (melhor prática de segurança)
USER airflow

# Copiar diretório de DAGs para a imagem
COPY dags /opt/airflow/dags
