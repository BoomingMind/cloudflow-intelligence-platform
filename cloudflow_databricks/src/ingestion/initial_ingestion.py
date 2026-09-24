from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


source = "/Volumes/cloudflow/landing/source_data/initial/"


datasets = [
    "customers",
    "products",
    "tickets",
    "agents",
    "knowledge_articles"
]


for dataset in datasets:

    df = (
        spark.read
        .format("json")
        .load(f"{source}/{dataset}")
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"cloudflow.bronze.{dataset}"
        )
    )