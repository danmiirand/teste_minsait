import os
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_replace
from pyspark.sql.types import DateType
import duckdb


# Validar a existência dos caminhos necessários
if not os.path.exists(java_path):
    raise EnvironmentError(f"Java não encontrado em: {java_path}")
if not os.path.exists(os.path.join(spark_root, "bin", "spark-submit")):
    raise EnvironmentError(f"Spark não localizado em: {spark_root}")


# Iniciar uma sessão Spark customizada
spark = SparkSession.builder \
    .appName("Health Data Processing Pipeline") \
    .master("local[*]") \
    .config("spark.sql.debug.maxToStringFields", "500") \
    .getOrCreate()

# Coletar todos os arquivos JSON no diretório de entrada
input_files = sorted(glob.glob("minsait/dados/*.json"))
if not input_files:
    raise FileNotFoundError("Nenhum arquivo JSON encontrado em 'minsait/dados/*.json'")
batch_size = 1000

# Inicializar tabelas temporárias em Pandas
patients_table = pd.DataFrame(columns=["pid", "gender", "birth"])
conditions_table = pd.DataFrame(columns=["cid", "patient_ref", "diagnosis", "record_date"])
medications_table = pd.DataFrame(columns=["mid", "patient_link", "drug_name", " prescription_date"])

# Função para processar um conjunto de arquivos
def process_file_set(files_to_process):
    global patients_table, conditions_table, medications_table

    # Ler os arquivos JSON como um DataFrame Spark
    data = spark.read.option("multiline", "true").json(files_to_process)

    # Separar os registros da coluna 'entry'
    records = data.select(explode(col("entry")).alias("item"))

    # Extrair dados de pacientes
    patient_data = records.filter(col("item.resource.resourceType") == "Patient").select(
        col("item.resource.id").alias("pid"),
        col("item.resource.gender").alias("gender"),
        col("item.resource.birthDate").cast(DateType()).alias("birth")
    )

    # Extrair dados de condições médicas
    condition_data = records.filter(col("item.resource.resourceType") == "Condition").select(
        col("item.resource.id").alias("cid"),
        regexp_replace(col("item.resource.subject.reference"), "urn:uuid:", "").alias("patient_ref"),
        col("item.resource.code.text").alias("diagnosis"),
        col("item.resource.recordedDate").cast(DateType()).alias("record_date")
    )

    # Extrair dados de prescrições de medicamentos
    medication_data = records.filter(col("item.resource.resourceType") == "MedicationRequest").select(
        col("item.resource.id").alias("mid"),
        regexp_replace(col("item.resource.subject.reference"), "urn:uuid:", "").alias("patient_link"),
        col("item.resource.medicationCodeableConcept.text").alias("drug_name"),
        col("item.resource.authoredOn").cast(DateType()).alias("prescription_date")
    )

    # Converter para Pandas e consolidar
    patients_chunk = patient_data.toPandas()
    conditions_chunk = condition_data.toPandas()
    medications_chunk = medication_data.toPandas()

    # Atualizar tabelas globais, removendo duplicatas
    patients_table = pd.concat([patients_table, patients_chunk]).drop_duplicates(subset=["pid"])
    conditions_table = pd.concat([conditions_table, conditions_chunk]).drop_duplicates(subset=["cid"])
    medications_table = pd.concat([medications_table, medications_chunk]).drop_duplicates(subset=["mid"])

# Processar os arquivos em grupos
for start_idx in range(0, len(input_files), batch_size):
    end_idx = min(start_idx + batch_size, len(input_files))
    file_batch = input_files[start_idx:end_idx]
    print(f"Iniciando lote de {start_idx} a {end_idx - 1} com {len(file_batch)} arquivos")
    process_file_set(file_batch)

# Exibir resumo dos resultados
print("\nResumo do processamento:")
print(f"Pacientes processados: {len(patients_table)} entradas")
print(f"Condições registradas: {len(conditions_table)} entradas")
print(f"Prescrições computadas: {len(medications_table)} entradas")

# Preparar o diretório para o DuckDB no contêiner
output_dir = "/opt/metabaseduck"
if not os.path.exists(output_dir):
    os.makedirs(output_dir, exist_ok=True)
    print(f"Criado diretório de saída: {output_dir}")
else:
    print(f"Usando diretório existente: {output_dir}")

# Exportar os dados para o DuckDB
db_file = os.path.join(output_dir, "health_data.duckdb")
connection = duckdb.connect(db_file)
connection.execute("CREATE OR REPLACE TABLE patients_table AS SELECT * FROM patients_table")
connection.execute("CREATE OR REPLACE TABLE conditions_table AS SELECT * FROM conditions_table")
connection.execute("CREATE OR REPLACE TABLE medication_table AS SELECT * FROM medications_table")


# Finalizar conexões
connection.close()
spark.stop()

print(f"Processamento concluído. Dados salvos em '{db_file}'. Use '/data/health_data.duckdb' no Metabase.")
