from pyspark.sql import DataFrame
from pyspark.sql.functions import min, max, avg


def aggregate_group_statistics(valid_df: DataFrame) -> DataFrame:
    metrics = ["mol_weight", "logp", "tpsa", "hbd", "hba"]

    agg_exprs = []
    for m in metrics:
        agg_exprs.extend([
            min(f"props.{m}").alias(f"{m}_min"),
            max(f"props.{m}").alias(f"{m}_max"),
            avg(f"props.{m}").alias(f"{m}_avg"),
        ])

    return (
        valid_df
        .groupBy("group_id")
        .agg(*agg_exprs)
    )
