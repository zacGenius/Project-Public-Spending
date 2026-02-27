import logging
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
silver_path = os.path.join(BASE_DIR, 'data', 'silver', 'gastos.parquet')

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_DB       = os.getenv("POSTGRES_DB", "public_spend")
PG_USER     = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def banco():
    logger.info(f"Verificando se banco '{PG_DB}' existe...")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname="postgres",
        user=PG_USER,
        password=PG_PASSWORD
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DB,))
    existe = cur.fetchone()

    if not existe:
        logger.info(f"Banco '{PG_DB}' não encontrado. Criando...")
        cur.execute(f"CREATE DATABASE {PG_DB}")
        logger.info(f"Banco '{PG_DB}' criado com sucesso.")
    else:
        logger.info(f"Banco '{PG_DB}' já existe. Continuando...")

    cur.close()
    conn.close()


def garantir_schema_e_tabela():
    logger.info("Verificando schema e tabela...")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.gastos (
            orgao_superior  TEXT,
            orgao           TEXT,
            valor_pago      NUMERIC(15, 2),
            data_referencia DATE,
            ano_referencia  INTEGER
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Schema e tabela verificados/criados com sucesso.")


def carregar_para_postgres():
    logger.info("=== INÍCIO DA CARGA POSTGRESQL ===")

    # 1. Garante banco antes de o Spark tentar conectar
    banco()

    # 2. Garante schema e tabela
    garantir_schema_e_tabela()

    # 3. Spark carrega os dados
    spark = (
        SparkSession.builder
        .appName("GastosPublicos_SilverToPostgres")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    df = spark.read.parquet(silver_path)
    logger.info(f"Linhas a Carregar: {df.count()}")

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "silver.gastos") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    logger.info("Carga Concluida com Sucesso")
    logger.info("=== FIM DA CARGA POSTGRESQL ===")


if __name__ == "__main__":
    carregar_para_postgres()