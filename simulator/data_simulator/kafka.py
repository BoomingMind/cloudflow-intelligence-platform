"""Aiven Kafka connection settings and a small event consumer."""
import argparse
import json
import os

from .common import load_environment, resolve


def _certificate_path(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Set {name} to the downloaded Aiven certificate file path")
    path = resolve(value)
    if not path.is_file():
        raise FileNotFoundError(f"{name} does not point to a file: {path}")
    return str(path)


def connection_options(service_uri=None):
    """Translate Aiven Quick connect fields into kafka-python options, without connecting."""
    load_environment()
    service_uri = service_uri or os.getenv("AIVEN_KAFKA_SERVICE_URI")
    if not service_uri:
        raise ValueError("Set AIVEN_KAFKA_SERVICE_URI to the Aiven Service URI (host:port)")
    if "://" in service_uri:
        raise ValueError("AIVEN_KAFKA_SERVICE_URI must be host:port, without a URL scheme")
    method = os.getenv("AIVEN_KAFKA_AUTH_METHOD", "sasl").strip().lower()
    options = {"bootstrap_servers": service_uri,
               "ssl_cafile": _certificate_path("AIVEN_KAFKA_CA_CERT_PATH")}
    if method == "sasl":
        username = os.getenv("AIVEN_KAFKA_SASL_USERNAME")
        password = os.getenv("AIVEN_KAFKA_SASL_PASSWORD")
        if not username or not password:
            raise ValueError("SASL requires AIVEN_KAFKA_SASL_USERNAME and AIVEN_KAFKA_SASL_PASSWORD")
        options.update(security_protocol="SASL_SSL",
                       sasl_mechanism="SCRAM-SHA-256",
                       sasl_plain_username=username,
                       sasl_plain_password=password)
    elif method == "client_certificate":
        options.update(security_protocol="SSL",
                       ssl_certfile=_certificate_path("AIVEN_KAFKA_SERVICE_CERT_PATH"),
                       ssl_keyfile=_certificate_path("AIVEN_KAFKA_SERVICE_ACCESS_KEY_PATH"))
    else:
        raise ValueError("AIVEN_KAFKA_AUTH_METHOD must be sasl or client_certificate")
    return options


def producer(service_uri=None):
    from kafka import KafkaProducer
    return KafkaProducer(**connection_options(service_uri), acks="all", retries=3,
                         key_serializer=lambda value: value.encode("utf-8"),
                         value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"))


def consume(service_uri, topic, limit):
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic, **connection_options(service_uri), auto_offset_reset="earliest",
                             group_id=None, consumer_timeout_ms=15000,
                             key_deserializer=lambda value: value.decode("utf-8") if value else None,
                             value_deserializer=lambda value: json.loads(value.decode("utf-8")))
    count = 0
    try:
        for message in consumer:
            print(json.dumps({"key": message.key, "event": message.value}, sort_keys=True))
            count += 1
            if count >= limit:
                break
    finally:
        consumer.close()
    return count


def main():
    load_environment()
    p = argparse.ArgumentParser()
    p.add_argument("--service-uri", default=os.getenv("AIVEN_KAFKA_SERVICE_URI"))
    p.add_argument("--topic", default=os.getenv("AIVEN_KAFKA_TOPIC", "application-events"))
    p.add_argument("--limit", type=int, default=5)
    args = p.parse_args()
    print(f"Consumed {consume(args.service_uri, args.topic, args.limit)} events")


if __name__ == "__main__":
    main()
