from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator

import logging
import os


default_args = {
    'owner': 'data_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def _locate_csv(**context):
    """
    Valida se o arquivo CSV existe e passa o caminho para as próximas tarefas.
    Se não encontrar o arquivo, falha imediatamente (fail-fast).
    """
    csv_path = '/opt/airflow/data/financial_market_750k.csv'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'Arquivo CSV não encontrado em {csv_path}')
    # Armazena o caminho em XCom para que outras tarefas acessem
    context['ti'].xcom_push(key='csv_path', value=csv_path)


def _load_staging(**context):
    """
    Carrega dados do CSV direto para a tabela staging usando COPY.
    COPY é extremamente eficiente — processa 750K registros em segundos.
    Limpa a tabela antes de carregar (sempre começa limpo).
    """
    csv_path = context['ti'].xcom_pull(key='csv_path', task_ids='locate_csv')
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Limpa dados antigos
            cur.execute('TRUNCATE TABLE staging;')
            # Carrega novo arquivo CSV
            with open(csv_path, 'r', encoding='utf-8') as data_file:
                cur.copy_expert(
                    "COPY staging (date, symbol, open, high, low, close, volume) FROM STDIN WITH CSV HEADER",
                    data_file,
                )


def _report_top_volatility(**context):
    """
    Busca o ativo com maior volatilidade média da semana.
    Gera um relatório resumido para informar decisões de risco.
    """
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    df = hook.get_pandas_df(
        """
        SELECT ticker,
               AVG(vol) AS avg_volatility
        FROM volatility_weekly
        GROUP BY ticker
        ORDER BY avg_volatility DESC
        LIMIT 1;
        """
    )
    if df.empty:
        message = 'Nenhum dado de volatilidade disponível.'
    else:
        top = df.iloc[0]
        # Cria mensagem com o ticker mais volátil
        message = (
            f"Ativo {top['ticker']} registrou maior volatilidade média "
            f"({top['avg_volatility']:.2f}%) na semana. Recomenda-se revisar hedge e limites."
        )
    # Armazena mensagem para próxima tarefa
    context['ti'].xcom_push(key='report_message', value=message)


def _log_execution_summary(**context):
    """
    Extrai o resumo gerado e registra no log.
    Log estruturado deixa rastreabilidade para auditoria posterior.
    """
    message = context['ti'].xcom_pull(key='report_message', task_ids='report_top_volatility')
    # Registra no log do Airflow para consulta rápida
    logging.getLogger(__name__).info('Resumo executivo: %s', message)


with DAG(
    dag_id='financial_volatility_pipeline',
    description='Pipeline diário que calcula a volatilidade semanal de ativos financeiros.',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # Executa diariamente às 7h da manhã
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Não reexecuta pipelines antigos
) as dag:
    setup_staging_table = PostgresOperator(
        task_id='setup_staging_table',
        postgres_conn_id='postgres_dw',
        sql="""
        -- Cria tabela staging se não existir (idempotente)
        CREATE TABLE IF NOT EXISTS staging (
            date DATE,
            symbol VARCHAR(10),
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT
        );
        """,
    )

    locate_csv = PythonOperator(
        task_id='locate_csv',
        python_callable=_locate_csv,
    )

    load_staging = PythonOperator(
        task_id='load_staging',
        python_callable=_load_staging,
    )

    run_data_quality_checks = SQLCheckOperator(
        task_id='run_data_quality_checks',
        conn_id='postgres_dw',
        sql="""
        -- Verifica se foram carregados exatamente 750K registros e sem nulos críticos
        SELECT
            COUNT(*) = 750000
            AND SUM(CASE WHEN close IS NULL OR date IS NULL THEN 1 ELSE 0 END) = 0
        FROM staging;
        """,
    )

    create_dim_tables = PostgresOperator(
        task_id='create_dim_tables',
        postgres_conn_id='postgres_dw',
        sql="""
        -- Dimensão de Instrumento: cada ativo único
        CREATE TABLE IF NOT EXISTS dim_instrumento (
            ticker VARCHAR(10) PRIMARY KEY,
            nome_ativo VARCHAR(50),
            tipo_ativo VARCHAR(20)
        );
        INSERT INTO dim_instrumento (ticker, nome_ativo, tipo_ativo)
        SELECT DISTINCT symbol, 'Ativo ' || symbol, 'Acao' FROM staging
        ON CONFLICT (ticker) DO NOTHING;

        -- Dimensão de Tempo: cada data única com hierarquia
        CREATE TABLE IF NOT EXISTS dim_tempo (
            data_id DATE PRIMARY KEY,
            ano INT,
            mes INT,
            dia_da_semana INT
        );
        INSERT INTO dim_tempo (data_id, ano, mes, dia_da_semana)
        SELECT DISTINCT date, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), EXTRACT(DOW FROM date) FROM staging
        ON CONFLICT (data_id) DO NOTHING;
        """,
    )

    load_fact_table = PostgresOperator(
        task_id='load_fact_table',
        postgres_conn_id='postgres_dw',
        sql="""
        -- Tabela Fato: movimento diário de cada ativo com variação calculada
        CREATE TABLE IF NOT EXISTS fact_movimentacao_diaria (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) REFERENCES dim_instrumento(ticker),
            data_id DATE REFERENCES dim_tempo(data_id),
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            variacao_diaria NUMERIC
        );
        -- Limpa dados da execução anterior
        TRUNCATE TABLE fact_movimentacao_diaria;
        -- Calcula variação diária (% de mudança desde o dia anterior)
        INSERT INTO fact_movimentacao_diaria (ticker, data_id, open, high, low, close, volume, variacao_diaria)
        SELECT s.symbol,
               s.date,
               s.open,
               s.high,
               s.low,
               s.close,
               s.volume,
               (s.close - LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date))
               / NULLIF(LAG(s.close) OVER (PARTITION BY s.symbol ORDER BY s.date), 0) * 100
        FROM staging s;
        """,
    )

    calculate_volatility_view = PostgresOperator(
        task_id='calculate_volatility_view',
        postgres_conn_id='postgres_dw',
        sql="""
        -- Cria view materializada: volatilidade semanal (desvio padrão da variação)
        CREATE MATERIALIZED VIEW IF NOT EXISTS volatility_weekly AS
        SELECT ticker,
               DATE_TRUNC('week', data_id) AS week,
               STDDEV_SAMP(variacao_diaria) AS vol
        FROM fact_movimentacao_diaria
        WHERE variacao_diaria IS NOT NULL
        GROUP BY ticker, DATE_TRUNC('week', data_id);

        -- Atualiza dados da view (já tem dados novos da tabela fato)
        REFRESH MATERIALIZED VIEW volatility_weekly;
        """,
    )

    generate_report = PythonOperator(
        task_id='report_top_volatility',
        python_callable=_report_top_volatility,
    )

    notify_executives = PythonOperator(
        task_id='log_execution_summary',
        python_callable=_log_execution_summary,
    )

    # Define fluxo: cada >> significa "depois disso, execute"
    setup_staging_table >> locate_csv >> load_staging >> run_data_quality_checks >> create_dim_tables >> load_fact_table >> calculate_volatility_view >> generate_report >> notify_executives