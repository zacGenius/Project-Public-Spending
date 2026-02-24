from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType, IntegerType
)

bronze_schema = StructType([
    StructField("orgaoSuperior", StringType(), True),
    StructField("orgao", StringType(), True),
    StructField("empenhado", StringType(), True),
    StructField("liquidado", StringType(), True),
    StructField("pago", StringType(), True),
    StructField("dataReferencia", StringType(), True)
])

silver_schema = StructType([
    StructField("orgao_superior", StringType(), False),
    StructField("orgao", StringType(), False),
    StructField("empenhado", DoubleType(), False),
    StructField("liquidado", DoubleType(), False),
    StructField("pago", DoubleType(), False),
    StructField("data_referencia", DateType(), False),
    StructField("ano_referencia", IntegerType(), False)
])

