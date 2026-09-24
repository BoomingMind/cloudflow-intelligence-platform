from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


SOURCE_BASE = (
    "/Volumes/cloudflow/landing/source_data/batch"
)


CHECKPOINT_BASE = (
    "/Volumes/cloudflow/ops/checkpoints/batch"
)


datasets = [
    "tickets",
    "customers",
    "knowledge_articles"
]


for dataset in datasets:

    print(
        f"Starting Auto Loader for {dataset}"
    )


    input_path = (
        f"{SOURCE_BASE}/{dataset}"
    )


    checkpoint_path = (
        f"{CHECKPOINT_BASE}/{dataset}"
    )


    df = (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json"
        )
        .option(
            "cloudFiles.schemaLocation",
            f"{checkpoint_path}/schema"
        )
        .load(input_path)
    )


    (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .outputMode("append")
        .toTable(
            f"cloudflow.bronze.{dataset}"
        )
    )