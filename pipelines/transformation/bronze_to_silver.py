import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, trim, upper, regexp_replace, year
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bronze_path = "../../data/bronze"
silver_path = "../../data/silver"

def spark_session():
    return (
        SparkSession.builder
        .appName("public_spend")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def most_recent_data(pasta: str) -> str:
    # Garante que o diretório existe antes de listar
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
        regexp_replace(col(coluna), r"\.", "")  # Remove ponto de milhar
    ).withColumn(
        coluna,
        regexp_replace(col(coluna), ",", ".")    # Troca vírgula por ponto
    ).withColumn(
        coluna,
        col(coluna).cast("double")               # Converte para número
    )

def normalize_organization_name(df, coluna: str):
    return df.withColumn(
        coluna,
        upper(trim(col(coluna)))
    )

def standardize_date(df, coluna: str):
    return df.withColumn(
        coluna,
        F.coalesce(
            to_date(col(coluna), "dd/MM/yyyy"),
            to_date(col(coluna), "yyyy-MM-dd"),
            to_date(col(coluna), "dd-MM-yyyy")
        )
    )

def remove_nulls(df):
    total_before = df.count()
    # Filtra as linhas nulas
    df_filtered = df.filter(
        col("orgao").isNotNull() &
        col("empenhado").isNotNull() &
        col("liquidado").isNotNull() &
        col("pago").isNotNull() &
        col("data_referencia").isNotNull() 
    )
    
    total_after = df_filtered.count()
    removidos = total_before - total_after

    if removidos > 0:
        logger.warning(f"{removidos} linhas removidas por nulos críticos")
    
    return df_filtered  # Retorno movido para fora do IF

def exec_transformation():
    logger.info("===== Start Transformation =====")

    spark = spark_session()

    # 1. Ler dado bruto
    try:
        arquivo = most_recent_data(bronze_path)
        # Corrigido: 'arquivo' em vez de 'arquivos'
        df = spark.read.csv(arquivo, header=True, inferSchema=False, encoding="utf-8")
    except Exception as e:
        logger.error(f"Erro ao ler arquivo: {e}")
        return

    logger.info(f"Linhas lidas: {df.count()}")

    # 2. Renomear colunas (Removidas as barras invertidas desnecessárias e corrigida a indentação)
    df = df.withColumnRenamed("orgaoSuperior", "orgao_superior") \
           .withColumnRenamed("valorPago", "valor_pago") \
           .withColumnRenamed("dataReferencia", "data_referencia")

    # 3. Limpeza e Tipagem
    df = clean_monetary_value(df, "empenhado")
    df = clean_monetary_value(df, "liquidado")
    df = clean_monetary_value(df, "pago")
    df = standardize_date(df, "data_referencia")
    df = normalize_organization_name(df, "orgao")
    df = normalize_organization_name(df, "orgao_superior")

    # 6. Criar coluna de ano
    df = df.withColumn("ano_referencia", year(col("data_referencia")))

    # 7. Remover nulos críticos
    df = remove_nulls(df)

    # 8. Remover Duplicatas 
    df = df.dropDuplicates()

    logger.info(f"Linhas após limpeza: {df.count()}")

    # 9. Salvar como parquet
    df.write.mode("overwrite").parquet(silver_path)

    logger.info(f"Salvo em Silver: {silver_path}")
    logger.info("===== End of Transformation =====")

if __name__ == "__main__":
    exec_transformation()
