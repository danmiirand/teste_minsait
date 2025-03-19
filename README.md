# Documentação: Arquitetura da Pipeline de Dados de Saúde

## Visão Geral
Esta pipeline foi projetada para processar dados de saúde em formato JSON (baseados no padrão FHIR), utilizando ferramentas modernas de ETL, armazenamento e visualização. O fluxo inicia com a análise exploratória no Jupyter Notebook, passa por um processo ETL com PySpark, armazena os dados estruturados no DuckDB, é orquestrado pelo Apache Airflow, e culmina na visualização interativa no Metabase. Tudo é conteinerizado com Docker para garantir portabilidade e consistência.

### Objetivos
- Analisar e processar dados FHIR (Patient, Condition, MedicationRequest).
- Estruturar os dados em tabelas otimizadas para consultas analíticas.
- Automatizar o processamento com agendamento.
- Disponibilizar dashboards interativos.

---

## Arquitetura

### Diagrama da Arquitetura
```
+--------------------+          +------------------+          +-----------------+
| Jupyter Notebook   |--------->| PySpark (ETL)    |--------->| DuckDB          |
| (Análise Inicial)  |          | spark_processor.py|          | (Armazenamento) |
+--------------------+          +------------------+          +-----------------+
                                    |                            |
                                    v                            v
+--------------------+          +------------------+          +-----------------+
| Apache Airflow     |<---------| Docker Compose   |--------->| Metabase        |
| (Orquestração)     |          | (Build/Deploy)   |          | (Visualização)  |
+--------------------+          +------------------+          +-----------------+
                                    |
                                    v
+--------------------+
| PostgreSQL         |
| (Metadados Airflow)|
+--------------------+
```

#### Fluxo de Dados
1. **Jupyter Notebook**: Análise exploratória inicial dos dados JSON.
2. **PySpark**: Extrai, transforma e carrega os dados em tabelas DuckDB.
3. **DuckDB**: Armazena os dados estruturados em `health_data.duckdb`.
4. **Airflow**: Orquestra a execução do script PySpark via DAG.
5. **Metabase**: Conecta-se ao DuckDB para visualização.
6. **PostgreSQL**: Armazena metadados do Airflow.

---

## Componentes Detalhados

### 1. Origem: Análise no Jupyter Notebook
- **Ferramenta**: Jupyter Notebook (instalado em `/usr/local/bin/jupyter` no WSL 2).
- **Propósito**: Exploração inicial dos arquivos JSON em `minsait/dados/`.
- **Processo**:
  - Os dados FHIR (pacientes, condições, prescrições) foram carregados e analisados em células interativas.
  - Identificou-se a estrutura dos dados e a lógica de extração (ex.: `entry.resource.resourceType`).
- **Saída**: Código Python inicial, posteriormente convertido para `spark_processor.py`.

### 2. ETL com PySpark
- **Motor**: Apache Spark com PySpark.
- **Script**: `spark_processor.py`.
- **Funcionalidade**:
  - **Extract**: Lê arquivos JSON em lotes (`minsait/dados/*.json`) com `spark.read.option("multiline", "true").json()`.
  - **Transform**: 
    - Explode a coluna `entry` para registros individuais.
    - Filtra por tipo de recurso (`Patient`, `Condition`, `MedicationRequest`).
    - Remove prefixos `urn:uuid:` das referências.
    - Converte datas para o tipo `DateType`.
    - Consolida em DataFrames Pandas, removendo duplicatas.
  - **Load**: Salva os dados em tabelas DuckDB.
- **Código Principal**:
  ```python
  spark = SparkSession.builder.appName("Health Data Processing Pipeline").master("local[*]").getOrCreate()
  data = spark.read.option("multiline", "true").json(input_files)
  records = data.select(explode(col("entry")).alias("item"))
  ```

### 3. Estruturação no DuckDB
- **Banco de Dados**: DuckDB (arquivo `health_data.duckdb` em `/opt/metabaseduck/` no contêiner).
- **Tabelas Criadas**:
  1. **patients_table**:
     - `pid` (VARCHAR): ID do paciente.
     - `gender` (VARCHAR): Gênero.
     - `birth` (DATE): Data de nascimento.
  2. **conditions_table**:
     - `cid` (VARCHAR): ID da condição.
     - `patient_ref` (VARCHAR): ID do paciente associado.
     - `diagnosis` (VARCHAR): Nome da condição.
     - `record_date` (DATE): Data de registro.
  3. **medication_table**:
     - `mid` (VARCHAR): ID da prescrição.
     - `patient_link` (VARCHAR): ID do paciente associado.
     - `drug_name` (VARCHAR): Nome do medicamento.
     - `prescription_date` (DATE): Data de prescrição.
- **Criação**:
  ```python
  connection = duckdb.connect(db_file)
  connection.execute("CREATE OR REPLACE TABLE patients AS SELECT * FROM patients_table")
  ```

### 4. Build e Deploy com Docker
- **Ferramenta**: Docker Compose.
- **Serviços**:
  - **Airflow**:
    - Imagem: Customizada via `Dockerfile.airflow` (baseada em `apache/airflow:2.9.0`).
    - Inclui Spark 3.5.1, Java 11, e dependências Python (`pyspark`, `pandas`, `duckdb`).
    - Portas: `8080` (webserver).
  - **Metabase**:
    - Imagem: Customizada via `Dockerfile.metabase` (baseada em `openjdk:19-buster`).
    - Inclui o driver DuckDB (`duckdb.metabase-driver.jar`).
    - Portas: `3000` ou `3001` (ajustado se necessário).
  - **PostgreSQL**:
    - Imagem: `postgres:13`.
    - Armazena metadados do Airflow.
    - Porta interna: `5432`.
- **Exemplo de `docker ps`**:
  ```
  CONTAINER ID   IMAGE                           COMMAND                  CREATED        STATUS       PORTS                    NAMES
  0c2d1e73d8f3   projeto-metabase                "java -jar /home/met…"   2 hours ago    Up 2 hours   0.0.0.0:3001->3000/tcp   projeto-metabase-1
  160cf5b42a73   apache/airflow:2.9.0            "/usr/bin/dumb-init …"   2 hours ago    Up 2 hours   8080/tcp                 projeto-airflow-scheduler-1
  25e00e2c474c   apache/airflow:2.9.0            "/usr/bin/dumb-init …"   2 hours ago    Up 2 hours   0.0.0.0:8080->8080/tcp   projeto-airflow-webserver-1
  e69e23d3fef6   postgres:13                     "docker-entrypoint.s…"   2 hours ago    Up 2 hours   5432/tcp                 projeto-postgres-1
  67cd84e8fddc   metabase-duckdb                 "java -jar /home/met…"   3 hours ago    Up 3 hours   0.0.0.0:3000->3000/tcp   metabase
  cc65fee91921   portainer/portainer-ce:latest   "/portainer"             7 months ago   Up 7 hours   0.0.0.0:8000->8000/tcp   portainer
  ```
  - **Notas**: 
    - `projeto-metabase-1` usa a porta `3001` devido a conflitos com `metabase` (`3000`).
    - `portainer` é um extra no ambiente, não parte da pipeline.

### 5. Agendamento no Airflow
- **DAG**: `process_health_data` (em `dags/process_data_dag.py`).
- **Tarefa**: Executa `spark_processor.py` via `PythonOperator`.
- **Agendamento**: Diário (`schedule_interval=timedelta(days=1)`), a partir de 19/03/2025.
- **Código**:
  ```python
  with DAG('process_health_data', schedule_interval=timedelta(days=1), start_date=datetime(2025, 3, 19)) as dag:
      process_task = PythonOperator(task_id='run_spark_processing', python_callable=run_spark_processor)
  ```
- **Execução**:
  - Acessar `http://localhost:8080`, ativar o DAG e disparar manualmente.

### 6. Visualização no Metabase
- **Conexão**: Banco DuckDB em `/data/health_data.duckdb`.
- **Dashboards**:
  - **Top Condições**: `SELECT diagnosis, COUNT(*) FROM conditions_table GROUP BY diagnosis ORDER BY COUNT(*) DESC LIMIT 10` (gráfico de barras).
  - **Top Medicamentos**: `SELECT drug_name, COUNT(*) FROM medication_table GROUP BY drug_name ORDER BY COUNT(*) DESC LIMIT 10` (gráfico de barras).
  - **Distribuição de Gênero**: `SELECT gender, COUNT(*) FROM patients_table GROUP BY gender` (gráfico de pizza).

---

## Repositório GitHub
- **URL**: `https://github.com/danmiirand/teste_minsait.git`.
- **Conteúdo**:
  - `dags/process_data_dag.py`
  - `metabase_plugins/duckdb.metabase-driver.jar`
  - `Dockerfile.airflow`
  - `Dockerfile.metabase`
  - `spark_processor.py`
  - `docker-compose.yml`
  - `requirements.txt`
  - `README.md`
  - `.gitignore`

---

## Configuração e Execução

### Pré-requisitos
- WSL 2 (Ubuntu).
- Docker Desktop com integração WSL.
- Git.

### Passos
1. **Clonar o Repositório**:
   ```bash
   git clone https://github.com/danmiirand/teste_minsait.git
   cd teste_minsait
   ```
2. **Adicionar Dados**:
   - Coloque os arquivos JSON em `minsait/dados/`.
3. **Iniciar os Serviços**:
   ```bash
   docker compose up -d --build
   ```
4. **Configurar Airflow**:
   - Acesse `http://localhost:8080` (admin/admin).
   - Ative e dispare o DAG `process_health_data`.
5. **Configurar Metabase**:
   - Acesse `http://localhost:3000` (ou `3001`).
   - Conecte-se a `/data/health_data.duckdb`.

---
