from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


# Load secrets

bootstrap_servers = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="bootstrap_servers"
)

username = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="username"
)

password = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="password"
)

ca_certificate = dbutils.secrets.get(
    scope="cloudflow-kafka",
    key="ca_certificate_path"
)


kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,

    "subscribe":
        "cloudflow.application_events",

    "kafka.security.protocol":
        "SASL_SSL",

    "kafka.sasl.mechanism":
        "SCRAM-SHA-256",

    "kafka.ssl.ca.location":
        ca_certificate,

    "kafka.sasl.jaas.config":
        f"""
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="{username}"
        password="{password}";
        """
}


events_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)


parsed_events = (
    events_df
    .selectExpr(
        "CAST(value AS STRING) AS event_json",
        "timestamp AS kafka_timestamp"
    )
)


(
    parsed_events
    .writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        "/Volumes/cloudflow/ops/checkpoints/application_events"
    )
    .outputMode("append")
    .toTable(
        "cloudflow.bronze.application_events"
    )
)