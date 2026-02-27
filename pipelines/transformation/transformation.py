import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, trim, upper, regexp_replace, year
import os
import shutil
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Caminhos absolutos baseados na localização do arquivo 
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
bronze_path = os.path.join(BASE_DIR, 'data', 'bronze')
silver_path = os.path.join(BASE_DIR, 'data', 'silver')
silver_temp = os.path.join(silver_path, '_temp')
silver_final = os.path.join(silver_path, 'gastos.parquet')


def spark_session():
    return (
        SparkSession.builder
        .appName("public_spend")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def most_recent_data(pasta: str) -> str:
    if not os.path.exists(pasta):
        raise FileNotFoundError(f"Diretório não encontrado: {pasta}")
    arquivos = [
        f for f in os.listdir(pasta)
        if f.endswith(".csv") and f.startswith("gastos_")
    ]
    if not arquivos:
        raise FileNotFoundError(f"Nenhum CSV 'gastos_' encontrado em {pasta}")
    arquivos.sort(reverse=True)
    caminho = os.path.join(pasta, arquivos[0])
    logger.info(f"Arquivo selecionado: {caminho}")
    return caminho


def clean_monetary_value(df, coluna: str):
    return df.withColumn(
        coluna,
        regexp_replace(col(coluna), r"\.", "")
    ).withColumn(
        coluna,
        regexp_replace(col(coluna), ",", ".")
    ).withColumn(
        coluna,
        col(coluna).cast("double")
    )


def normalize_organization_name(df, coluna: str):
    return df.withColumn(
        coluna,
        upper(trim(col(coluna)))
    )


def remove_nulls(df):
    total_before = df.count()
    df_filtered = df.filter(
        col("orgao").isNotNull() &
        col("empenhado").isNotNull() &
        col("liquidado").isNotNull() &
        col("pago").isNotNull()
    )
    total_after = df_filtered.count()
    removidos = total_before - total_after
    if removidos > 0:
        logger.warning(f"{removidos} linhas removidas por nulos críticos")
    return df_filtered


def exec_transformation():
    logger.info("===== Start Transformation =====")
    spark = spark_session()

    # 1. Ler dado bruto
    try:
        arquivo = most_recent_data(bronze_path)
        df = spark.read.csv(arquivo, header=True, inferSchema=False, encoding="utf-8")
    except Exception as e:
        logger.error(f"Erro ao ler arquivo: {e}")
        return

    logger.info(f"Linhas lidas: {df.count()}")
    logger.info(f"Colunas disponíveis: {df.columns}")

    # 2. Renomear colunas
    df = df.withColumnRenamed("orgaoSuperior", "orgao_superior") \
           .withColumnRenamed("codigoOrgaoSuperior", "codigo_orgao_superior") \
           .withColumnRenamed("codigoOrgao", "codigo_orgao")

    # 3. Limpeza e tipagem 
    df = clean_monetary_value(df, "empenhado")
    df = clean_monetary_value(df, "liquidado")
    df = clean_monetary_value(df, "pago")

    # 4. Normalizar nomes de órgãos
    df = normalize_organization_name(df, "orgao")
    df = normalize_organization_name(df, "orgao_superior")

    # 5. Tipar ano
    df = df.withColumn("ano", col("ano").cast("integer"))

    # 6. Remover nulos e duplicatas 
    df = remove_nulls(df)
    df = df.dropDuplicates()

    logger.info(f"Linhas após limpeza: {df.count()}")

    # 7. Salvar como parquet único 
    os.makedirs(silver_path, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").parquet(silver_temp)

    part_file = glob.glob(os.path.join(silver_temp, 'part-*.parquet'))[0]

    if os.path.exists(silver_final):
        os.remove(silver_final)

    shutil.move(part_file, silver_final)
    shutil.rmtree(silver_temp)

    logger.info(f"Salvo em Silver: {silver_final}")
    logger.info("===== End of Transformation =====")


if __name__ == "__main__":
    exec_transformation()