from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.read_data import read_molecules, read_groups, write_table
from src.enrich_molecules import calculate_properties
from src.aggregations import aggregate_group_statistics

MOLECULES_PATH = "/Volumes/workspace/default/properties_calculation/molecules.csv"
GROUPS_PATH = "/Volumes/workspace/default/properties_calculation/groups.csv"


def run():
    spark = SparkSession.builder.appName("MoleculePropertiesPipeline").getOrCreate()

    molecules_df = read_molecules(spark, MOLECULES_PATH)
    groups_df = read_groups(spark, GROUPS_PATH)

    enriched_df = molecules_df.withColumn(
        "props",
        calculate_properties(col("smiles"))
    )

    valid_df = enriched_df.filter(col("props.mol_weight").isNotNull())
    invalid_df = enriched_df.filter(col("props.mol_weight").isNull())

    write_table(
        invalid_df.select("molecule_id", "smiles", "group_id"),
        "workspace.default.invalid_molecules"
    )

    valid_flat_df = valid_df.select(
        "molecule_id",
        "smiles",
        "group_id",
        col("props.mol_weight").alias("mol_weight"),
        col("props.logp").alias("logp"),
        col("props.tpsa").alias("tpsa"),
        col("props.hbd").alias("hbd"),
        col("props.hba").alias("hba"),
    )

    write_table(valid_flat_df, "workspace.default.valid_molecules")

    stats_df = aggregate_group_statistics(valid_df)

    final_df = groups_df.join(stats_df, "group_id", "left")

    write_table(final_df, "workspace.default.group_statistics")


run()
