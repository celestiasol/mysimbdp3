import csv
import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

from confluent_kafka import Producer
from dateutil import parser as date_parser

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
RAW_TOPIC_NAME = os.getenv("RAW_TOPIC_NAME", "azure.llm.raw")
INPUT_CSV_PATH = os.getenv("INPUT_CSV_PATH", "/app/data/azure_llm_inference.csv")
REPLAY_SPEED_MULTIPLIER = float(os.getenv("REPLAY_SPEED_MULTIPLIER", "50"))
LOOP_DATASET = os.getenv("LOOP_DATASET", "false").lower() == "true"
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))
ERROR_RATE = float(os.getenv("ERROR_RATE", "0.0"))
INPUT_SCHEMA_VERSION = os.getenv("INPUT_SCHEMA_VERSION", "1.0.0")

TIMESTAMP_CANDIDATES = ["Timestamp", "timestamp", "Time", "time", "Datetime", "datetime"]
SERVICE_CANDIDATES = [
    "ServiceName",
    "service_name",
    "DeploymentName",
    "deployment_name",
    "ModelDeployment",
    "ModelDeploymentName",
    "Deployment",
    "Model",
    "model",
]
CONTEXT_TOKEN_CANDIDATES = ["ContextTokens", "context_tokens", "PromptTokens", "InputTokens"]
GENERATED_TOKEN_CANDIDATES = ["GeneratedTokens", "generated_tokens", "CompletionTokens", "OutputTokens"]


def first_present(row: Dict[str, str], candidates: Iterable[str]) -> Optional[str]:
    for name in candidates:
        if name in row and row[name] not in (None, ""):
            return row[name]
    return None


def parse_ms(value: str) -> int:
    dt = date_parser.parse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def build_event_id(event: Dict[str, object]) -> str:
    stable_payload = json.dumps(
        {
            "event_time_ms": event.get("event_time_ms"),
            "service_name": event.get("service_name"),
            "context_tokens": event.get("context_tokens"),
            "generated_tokens": event.get("generated_tokens"),
            "source": event.get("source"),
            "schema_version": event.get("schema_version"),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()


def maybe_corrupt(event: Dict[str, object]) -> Dict[str, object]:
    if ERROR_RATE <= 0:
        event["event_id"] = build_event_id(event)
        return event
    copy = dict(event)
    if random.random() < ERROR_RATE:
        fault = random.choice(["missing_context", "missing_generated", "negative_tokens", "missing_time"])
        if fault == "missing_context":
            copy["context_tokens"] = None
        elif fault == "missing_generated":
            copy["generated_tokens"] = None
        elif fault == "negative_tokens":
            copy["generated_tokens"] = -1
        elif fault == "missing_time":
            copy["event_time_ms"] = None
    copy["event_id"] = build_event_id(copy)
    return copy


def normalize(row: Dict[str, str]) -> Dict[str, object]:
    timestamp_raw = first_present(row, TIMESTAMP_CANDIDATES)
    context_tokens_raw = first_present(row, CONTEXT_TOKEN_CANDIDATES)
    generated_tokens_raw = first_present(row, GENERATED_TOKEN_CANDIDATES)
    service_name_raw = first_present(row, SERVICE_CANDIDATES) or "unknown-service"

    event = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "event_time_ms": parse_ms(timestamp_raw) if timestamp_raw else None,
        "service_name": str(service_name_raw),
        "context_tokens": int(float(context_tokens_raw)) if context_tokens_raw not in (None, "") else None,
        "generated_tokens": int(float(generated_tokens_raw)) if generated_tokens_raw not in (None, "") else None,
        "source": "azure-llm-inference-dataset-2024",
        "ingested_at_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }
    return maybe_corrupt(event)


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def iter_rows(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row


def sleep_for_replay(previous_ts_ms: Optional[int], current_ts_ms: Optional[int]):
    if previous_ts_ms is None or current_ts_ms is None:
        return
    delta_ms = max(0, current_ts_ms - previous_ts_ms)
    if REPLAY_SPEED_MULTIPLIER <= 0:
        return
    time.sleep((delta_ms / 1000.0) / REPLAY_SPEED_MULTIPLIER)


def main():
    input_path = Path(INPUT_CSV_PATH)
    if not input_path.exists():
        raise FileNotFoundError(
            f"Dataset file not found at {INPUT_CSV_PATH}. Put the Azure CSV there before running compose."
        )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    sent = 0

    while True:
        previous_ts_ms = None
        for row in iter_rows(input_path):
            event = normalize(row)
            event_time_ms = event.get("event_time_ms")
            sleep_for_replay(previous_ts_ms, event_time_ms)
            producer.produce(
                RAW_TOPIC_NAME,
                key=str(event.get("service_name", "unknown")),
                value=json.dumps(event).encode("utf-8"),
                timestamp=event_time_ms,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            previous_ts_ms = event_time_ms
            sent += 1

            if sent % 1000 == 0:
                print(f"sent={sent}")
            if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
                producer.flush()
                print(f"finished after {sent} messages")
                return

        if not LOOP_DATASET:
            break

    producer.flush()
    print(f"finished after {sent} messages")


if __name__ == "__main__":
    main()
