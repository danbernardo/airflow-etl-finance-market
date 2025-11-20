
# Pipeline ETL de Análise de Volatilidade Financeira

Pipeline de dados em produção que processa 750 mil registros de cotações financeiras através de uma arquitetura Medallion (Bronze → Silver → Gold). Implementa orquestração robusta com Apache Airflow, data warehouse normalizado em PostgreSQL e Star Schema otimizado para analytics.

---

## Objetivo do Projeto

- Ingerir e processar 750 mil registros de dados de mercado financeiro com eficiência
- Calcular indicadores de volatilidade semanal por ativo
- Estruturar dados em Star Schema para consultas analíticas
- Garantir rastreabilidade e reprodutibilidade do pipeline

---

## Dashboard Interativo

Os dados consolidados estão disponíveis no PostgreSQL em tabelas normalizadas (Medallion Gold Layer) prontas para integração com ferramentas de BI como Power BI, Looker ou Grafana.

**Indicadores disponíveis**: volatilidade semanal, volume agregado, variação diária, correlações entre ativos.

---

## Tecnologias Utilizadas

| Componente | Tecnologia |
| --- | --- |
| Orquestração | Apache Airflow (DAGs) |
| Data Warehouse | PostgreSQL (Star Schema) |
| Processamento | Python, Pandas |
| Ingestão | COPY FROM nativo do PostgreSQL |
| Visualização | Análise exploratória em Jupyter Notebooks |
| Containerização | Docker Compose |
| Versionamento | GitHub |

---

## Fonte de Dados

Os dados utilizados no projeto foram extraídos de uma única fonte:

| Fonte | Conteúdo | Quantidade |
| --- | --- | --- |
| **CSV Local** | Cotações diárias de 750 mil registros (data, símbolo, open, high, low, close, volume) | 1 arquivo |

---

## Arquitetura de Dados – Medallion Architecture

O pipeline segue o modelo **Medallion Architecture** com três camadas de transformação:

### Bronze Layer – Ingestão
Dados brutos carregados do CSV via `COPY FROM` PostgreSQL (750K registros em ~2 segundos). Tabela `staging` mantém dados originais sem transformação.

### Silver Layer – Validação
Aplicação de regras de qualidade de dados e criação de dimensões normalizadas (`dim_instrument`, `dim_tempo`).

### Gold Layer – Analytics Ready
Tabelas fato (`fact_movimentacao_diaria`, `volatility_weekly`) estruturadas em Star Schema, otimizadas para queries analíticas.

---

-- Tabela Fato: Volatilidade Semanal
CREATE TABLE volatility_weekly AS
SELECT
    DATE_TRUNC('week', date)::date AS week_start,
    ticker,
    ROUND(STDDEV(variacao_diaria), 2) AS vol
FROM fact_movimentacao_diaria
GROUP BY DATE_TRUNC('week', date), ticker;
```

**Características:**
- Cálculo automático de volatilidade (desvio padrão)
- Agregações por semana e por ativo
- Indicadores de amplitude e variação diária
- Pronto para relatórios executivos

---

## Métricas-Chave

### � Volatilidade (Desvio Padrão da Variação Diária)

Mede **o quanto o preço de um ativo varia**. Ações com alta volatilidade têm preços que oscilam muito, indicando maior potencial de ganho e maior risco de perda.

```sql
SELECT 
    ticker,
    STDDEV(variacao_diaria) AS volatilidade,
    AVG(variacao_diaria) AS retorno_medio,
    MAX(variacao_diaria) AS variacao_maxima,
    MIN(variacao_diaria) AS variacao_minima
FROM fact_movimentacao_diaria
GROUP BY ticker
ORDER BY volatilidade DESC;
```

**Interpretação:**
- Valores **altos** indicam maior oscilação de preços (risco elevado, potencial alto).
- Valores **baixos** indicam estabilidade (menor risco, retornos previsíveis).

### Volume de Operações

Indica o volume total negociado por ativo, sinalizando liquidez e interesse do mercado.

```sql
SELECT 
    ticker,
    AVG(volume) AS volume_medio,
    SUM(volume) AS volume_total
FROM fact_movimentacao_diaria
GROUP BY ticker
ORDER BY volume_total DESC;
```

---

## Modelagem Star Schema – Tabelas Disponíveis

### Tabelas Fato

| Tabela | Descrição | Granularidade |
| --- | --- | --- |
| `fact_movimentacao_diaria` | Registros de abertura, fechamento, máxima, mínima, volume e variação diária | Diária por ativo |
| `volatility_weekly` | Volatilidade agregada por semana | Semanal por ativo |

### Tabelas Dimensão

- `dim_time`: Datas com hierarquia (ano, mês, semana, dia).
- `dim_instrument`: Tickers dos ativos com suas metadatas.

> **Observação**: A normalização em dimensões permite consultas otimizadas e reutilização em múltiplos relatórios.

---

## Fluxo de Transformação – Pipeline Airflow

### Concessões CSV → Staging → Fact/Dimension Tables

| Etapa | Responsável | Descrição |
| --- | --- | --- |
| **locate_csv** | Python Task | Valida existência do arquivo CSV (750k) |
| **load_staging** | PostgreSQL COPY | Carrega dados brutos na tabela staging |
| **data_quality** | SQL Check | Valida contagem de registros e nulos |
| **transform_dimensions** | SQL | Cria dimensões (tempo, instrumento) |
| **transform_facts** | SQL | Cria tabela fato e calcula volatilidade |
| **report_top_volatility** | Python | Extrai e analisa ativos mais voláteis |
| **log_execution_summary** | Logging | Registra resumo da execução |

**Características da Orquestração:**
- Retries automáticos (3 tentativas com delay de 5 minutos)
- XCom para passagem de contexto entre tasks
- Schedule diário às 7h da manhã
- Fail-fast em validações de qualidade

---

## Insights Destacados

- **Top 5 Ativos Mais Voláteis**: Identificados pela análise semanal e relatados automaticamente ao final de cada ciclo.
- **Correlações de Mercado**: Análise exploratória detecta padrões de movimento entre ativos.
- **Alertas de Risco**: Volatilidade acima de limiar dispara recomendação para hedge e revisão de posições.
- **Reprodutibilidade**: Todo cálculo é rastreável desde a ingestão até o relatório final.

---

## Estrutura de Pastas do Projeto

```
📦 financial-market-analysis
│
├── dags
│   └── financial_pipeline.py             # DAG Airflow com orquestração completa
│
├── sql
│   ├── staging.sql                       # Criação de tabela staging
│   ├── dim_time.sql                      # Dimensão de tempo
│   ├── dim_instrument.sql                # Dimensão de instrumento
│   ├── fact_movimentacao.sql             # Fato movimentação diária
│   ├── volatility_weekly.sql             # Agregação semanal de volatilidade
│   └── quality_checks.sql                # Validações de qualidade
│
├── analysis
│   ├── exploratory_analysis.ipynb        # Análise exploratória com Pandas e visualizações
│   └── volatility_report.ipynb           # Relatório de volatilidade e recomendações
│
├── scripts
│   └── [scripts auxiliares de ETL]
│
├── docker-compose.yml                    # Orquestração de Airflow + PostgreSQL
├── Dockerfile                            # Imagem customizada do Airflow
├── start_services.bat                    # Script para iniciar stack
├── financial_market_750k.csv             # Dataset de entrada (750k registros)
└── README.md                             # Documentação do projeto
```

---

## Como Reproduzir

### Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.9+
- PostgreSQL 12+ (será iniciado via Docker)
- Apache Airflow 2.0+ (será iniciado via Docker)

### Passos de Execução

1. **Clone ou navegue até o diretório do projeto:**
   ```bash
   cd c:\Users\danie\Downloads\project
   ```

2. **Configure o arquivo `.env`** (variáveis de ambiente seguras):
   
   Copie o arquivo de exemplo:
   ```bash
   copy .env.example .env
   ```
   
   Edite o arquivo `.env` com suas credenciais seguras:
   ```env
   # Gere novas chaves Fernet e SECRET_KEY para produção:
   # Fernet Key: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   
   POSTGRES_PASSWORD=sua_senha_segura_aqui
   AIRFLOW_WWW_USER_PASSWORD=sua_senha_webui_aqui
   AIRFLOW_FERNET_KEY=sua_chave_fernet_aqui
   AIRFLOW_SECRET_KEY=sua_chave_secreta_aqui
   AIRFLOW_CONN_POSTGRES_DW=postgresql://airflow:sua_senha_segura_aqui@postgres:5432/financial_dw
   AIRFLOW_DATABASE_SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:sua_senha_segura_aqui@postgres/airflow
   ```
   
   **IMPORTANTE**: O arquivo `.env` está no `.gitignore` e nunca será commitado no repositório.

3. **Inicie os serviços (Airflow + PostgreSQL):**
   ```bash
   docker compose down -v
   docker compose up -d
   ```

4. **Aguarde 30-60 segundos** para inicialização completa.

5. **Acesse o Airflow UI:**
   - URL: `http://localhost:58080` (ou `http://<WSL_IP>:58080` se rodando em WSL)
   - Usuário: (conforme definido em `AIRFLOW_WWW_USER_USERNAME` no `.env`)
   - Senha: (conforme definido em `AIRFLOW_WWW_USER_PASSWORD` no `.env`)

   **Exemplo de login (baseado em `.env`):**
   ```
   Usuário: admin1
   Senha: sua_senha_webui_aqui
   ```

6. **Ative a DAG `financial_volatility_pipeline`** no Airflow UI.

7. **Trigger manual ou aguarde o schedule** (diariamente às 7h).

8. **Consulte os resultados** no PostgreSQL ou nos notebooks Jupyter:
   ```bash
   jupyter notebook analysis/exploratory_analysis.ipynb
   ```

### Validação

Após a execução, consulte as tabelas Gold:
```sql
SELECT * FROM volatility_weekly ORDER BY week_start DESC LIMIT 10;
SELECT * FROM fact_movimentacao_diaria LIMIT 100;
```

---

## Login no Airflow UI – Fluxo Passo-a-Passo

### 1. Abra o navegador e acesse a URL

```
http://localhost:58080
```

> Se estiver rodando em WSL sem Docker Desktop, use o IP da máquina WSL:
> ```
> http://<seu_wsl_ip>:58080
> ```
> Para encontrar o IP: execute `hostname -I` dentro da WSL.

### 2. Você verá a tela de login do Airflow

A página exibirá dois campos:
- **Username** (ou Email)
- **Password**

### 3. Digite suas credenciais do `.env`

Use as variáveis que você configurou:

```
Username: admin1                          (valor de AIRFLOW_WWW_USER_USERNAME)
Password: sua_senha_webui_aqui            (valor de AIRFLOW_WWW_USER_PASSWORD)
```

**Exemplo completo baseado no `.env`:**
```env
AIRFLOW_WWW_USER_USERNAME=admin1
AIRFLOW_WWW_USER_PASSWORD=MinhaSenh@Segur@2025
```

Então o login seria:
```
Username: admin1
Password: MinhaSenh@Segur@2025
```

### 4. Clique em "Sign In"

Após autenticar com sucesso, você acessará o dashboard principal do Airflow onde pode:
- Visualizar DAGs
- Monitorar execuções
- Acionar pipelines manualmente
- Consultar logs

---

## Limpeza e Manutenção

### Logs (Airflow)

Os logs são armazenados em `logs/` e registram **todas as execuções da DAG**. São temporários e podem crescer bastante.

**Quando limpar:**
- Antes de compartilhar o repositório (contêm timestamps e IPs internos)
- Para liberar espaço em disco
- Quando quer "resetar" o histórico de execuções

**Como limpar:**

```bash
# Limpar todos os logs (mantém estrutura de pastas)
Remove-Item -Path "logs/*" -Recurse -Force

# Ou ao parar os containers
docker compose down -v
```
=======
# airflow-etl-finance-market
Pipeline ETL Mercado Financeiro com Airflow, Pandas e PostgreSQL — Exemplo didático.
>>>>>>> be23e7ccd61027f8f4b51f2438789b68f08a856b
