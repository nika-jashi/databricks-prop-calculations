from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

MOLECULES_SCHEMA = StructType([
    StructField("molecule_id", StringType(), False),
    StructField("smiles", StringType(), False),
    StructField("group_id", IntegerType(), False),
])

GROUPS_SCHEMA = StructType([
    StructField("group_id", IntegerType(), False),
    StructField("owner", StringType(), True),
    StructField("description", StringType(), True),
])


def read_molecules(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(MOLECULES_SCHEMA).option("header", True).csv(path)


def read_groups(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(GROUPS_SCHEMA).option("header", True).csv(path)


def write_table(df: DataFrame, table_name: str) -> None:
    (
        df.write
        .mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
