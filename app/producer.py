import os
import json
from loguru import logger
from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                                    default="localhost:9092")
bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS.split("|")

logger.info(f"KAFKA BOOTSTRAP SERVERS -> {bootstrap_servers}")


def json_serializer(data):
    return json.dumps(data).encode("UTF-8")


def publish_to_kafka(topic, value):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
    )
    try:
        logger.info(f"message publishing to kafka -> {value}")
        producer.send(topic=topic, value=value)
    except KafkaError as e:
        logger.error(
            f"Failed to publish record on to Kafka broker with error {e}"
        )
    else:
        logger.info("message publish to kafka successful")