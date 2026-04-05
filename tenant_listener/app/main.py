import json
import os

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
ALERTS_TOPIC_NAME = os.getenv("ALERTS_TOPIC_NAME", "azure.llm.alerts")
TENANT_GROUP_ID = os.getenv("TENANT_GROUP_ID", "tenant-alert-listener")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": TENANT_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([ALERTS_TOPIC_NAME])
    print(f"listening for tenant alerts on {ALERTS_TOPIC_NAME}")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer error: {msg.error()}")
            continue
        payload = json.loads(msg.value().decode("utf-8"))
        print(f"TENANT ALERT: {json.dumps(payload, sort_keys=True)}")


if __name__ == "__main__":
    main()
