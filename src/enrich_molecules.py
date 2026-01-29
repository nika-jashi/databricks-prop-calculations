import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, DoubleType, StructField
from src.descriptors import calculate_descriptors

schema = StructType([
    StructField("mol_weight", DoubleType()),
    StructField("logp", DoubleType()),
    StructField("tpsa", DoubleType()),
    StructField("hbd", DoubleType()),
    StructField("hba", DoubleType()),
])


@pandas_udf(schema)
def calculate_properties(smiles: pd.Series) -> pd.DataFrame:
    rows = []
    for s in smiles:
        result = calculate_descriptors(s)
        rows.append(result if result else (None, None, None, None, None))

    return pd.DataFrame(rows, columns=[
        "mol_weight", "logp", "tpsa", "hbd", "hba"
    ])
