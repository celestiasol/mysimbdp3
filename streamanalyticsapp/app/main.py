import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pymongo import MongoClient
from quixstreams import Application
from quixstreams.dataframe.windows.aggregations import Aggregator

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "azure-llm-analytics-cg")
RAW_TOPIC_NAME = os.getenv("RAW_TOPIC_NAME", "azure.llm.raw")
ANALYTICS_TOPIC_NAME = os.getenv("ANALYTICS_TOPIC_NAME", "azure.llm.analytics")
ALERTS_TOPIC_NAME = os.getenv("ALERTS_TOPIC_NAME", "azure.llm.alerts")
INVALID_TOPIC_NAME = os.getenv("INVALID_TOPIC_NAME", "azure.llm.invalid")

WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", "60"))
WINDOW_STEP_SEC = int(os.getenv("WINDOW_STEP_SEC", "30"))
LATE_GRACE_SEC = int(os.getenv("LATE_GRACE_SEC", "10"))
ALERT_REQUEST_COUNT_THRESHOLD = int(os.getenv("ALERT_REQUEST_COUNT_THRESHOLD", "100"))
ALERT_GENERATED_TOKENS_THRESHOLD = int(os.getenv("ALERT_GENERATED_TOKENS_THRESHOLD", "10000"))
ANALYTICS_SCHEMA_VERSION = os.getenv("ANALYTICS_SCHEMA_VERSION", "1.0.0")
PROCESSING_GUARANTEE = os.getenv("PROCESSING_GUARANTEE", "at-least-once")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "streamanalytics")

mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DATABASE]
analytics_collection = db["analytics_windows"]
alerts_collection = db["alerts"]
invalid_collection = db["invalid_records"]


class TokenMetricsAggregator(Aggregator):
    def initialize(self):
        return {
            "request_count": 0,
            "context_tokens_sum": 0,
            "generated_tokens_sum": 0,
            "max_generated_tokens": 0,
            "processing_latency_ms_sum": 0,
            "max_processing_latency_ms": 0,
        }

    def agg(self, old, new, timestamp):
        processing_latency_ms = max(0, int(time.time() * 1000) - int(new["ingested_at_ms"]))
        old["request_count"] += 1
        old["context_tokens_sum"] += int(new["context_tokens"])
        old["generated_tokens_sum"] += int(new["generated_tokens"])
        old["max_generated_tokens"] = max(old["max_generated_tokens"], int(new["generated_tokens"]))
        old["processing_latency_ms_sum"] += processing_latency_ms
        old["max_processing_latency_ms"] = max(old["max_processing_latency_ms"], processing_latency_ms)
        return old

    def result(self, stored):
        count = max(stored["request_count"], 1)
        return {
            "request_count": stored["request_count"],
            "context_tokens_sum": stored["context_tokens_sum"],
            "generated_tokens_sum": stored["generated_tokens_sum"],
            "context_tokens_avg": stored["context_tokens_sum"] / count,
            "generated_tokens_avg": stored["generated_tokens_sum"] / count,
            "max_generated_tokens": stored["max_generated_tokens"],
            "avg_processing_latency_ms": stored["processing_latency_ms_sum"] / count,
            "max_processing_latency_ms": stored["max_processing_latency_ms"],
        }


def timestamp_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: Any,
) -> int:
    return int(value["event_time_ms"])


def invalid_reason(event: Dict[str, Any]) -> Optional[str]:
    required = ["event_id", "schema_version", "event_time_ms", "service_name", "context_tokens", "generated_tokens", "ingested_at_ms"]
    for field in required:
        if field not in event or event[field] is None:
            return f"missing:{field}"
    if int(event["context_tokens"]) < 0:
        return "negative:context_tokens"
    if int(event["generated_tokens"]) < 0:
        return "negative:generated_tokens"
    return None


def is_valid(event: Dict[str, Any]) -> bool:
    return invalid_reason(event) is None


def to_invalid_record(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": event.get("event_id"),
        "schema_version": event.get("schema_version"),
        "received_at_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "reason": invalid_reason(event),
        "raw_record": event,
    }


def build_window_id(record: Dict[str, Any]) -> str:
    material = f"{record['service_name']}|{record['window_start_ms']}|{record['window_end_ms']}|{record['schema_version']}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def to_window_record(value: Dict[str, Any], key: Any, timestamp: int, headers: Any) -> Dict[str, Any]:
    metrics = value["metrics"]
    record = {
        "schema_version": ANALYTICS_SCHEMA_VERSION,
        "service_name": key,
        "window_start_ms": int(value["start"]),
        "window_end_ms": int(value["end"]),
        "window_duration_sec": WINDOW_SIZE_SEC,
        "window_step_sec": WINDOW_STEP_SEC,
        "grace_sec": LATE_GRACE_SEC,
        "request_count": metrics["request_count"],
        "context_tokens_sum": metrics["context_tokens_sum"],
        "generated_tokens_sum": metrics["generated_tokens_sum"],
        "context_tokens_avg": metrics["context_tokens_avg"],
        "generated_tokens_avg": metrics["generated_tokens_avg"],
        "max_generated_tokens": metrics["max_generated_tokens"],
        "avg_processing_latency_ms": metrics["avg_processing_latency_ms"],
        "max_processing_latency_ms": metrics["max_processing_latency_ms"],
        "throughput_rps": metrics["request_count"] / max(WINDOW_SIZE_SEC, 1),
        "updated_at_ms": int(timestamp),
    }
    record["window_id"] = build_window_id(record)
    return record


def persist_window(record: Dict[str, Any]):
    analytics_collection.update_one(
        {
            "service_name": record["service_name"],
            "window_start_ms": record["window_start_ms"],
            "window_end_ms": record["window_end_ms"],
        },
        {"$set": record},
        upsert=True,
    )


def persist_invalid(record: Dict[str, Any]):
    invalid_collection.update_one(
        {"event_id": record.get("event_id"), "reason": record.get("reason")},
        {"$set": record},
        upsert=True,
    )


def persist_alert(record: Dict[str, Any]):
    alerts_collection.update_one(
        {"alert_id": record["alert_id"]},
        {"$set": record},
        upsert=True,
    )


def is_alert(record: Dict[str, Any]) -> bool:
    return (
        record["request_count"] >= ALERT_REQUEST_COUNT_THRESHOLD
        or record["generated_tokens_sum"] >= ALERT_GENERATED_TOKENS_THRESHOLD
    )


def to_alert_record(record: Dict[str, Any]) -> Dict[str, Any]:
    alert_type = []
    if record["request_count"] >= ALERT_REQUEST_COUNT_THRESHOLD:
        alert_type.append("request_count")
    if record["generated_tokens_sum"] >= ALERT_GENERATED_TOKENS_THRESHOLD:
        alert_type.append("generated_tokens_sum")
    alert_record = {
        **record,
        "alert_type": "+".join(alert_type),
        "alert_created_at_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }
    alert_material = f"{alert_record['window_id']}|{alert_record['alert_type']}"
    alert_record["alert_id"] = hashlib.sha256(alert_material.encode("utf-8")).hexdigest()
    return alert_record


app = Application(
    broker_address=KAFKA_BOOTSTRAP_SERVERS,
    consumer_group=KAFKA_CONSUMER_GROUP,
    processing_guarantee=PROCESSING_GUARANTEE,
)

input_topic = app.topic(
    RAW_TOPIC_NAME,
    value_deserializer="json",
    timestamp_extractor=timestamp_extractor,
)
analytics_topic = app.topic(ANALYTICS_TOPIC_NAME, value_serializer="json")
alerts_topic = app.topic(ALERTS_TOPIC_NAME, value_serializer="json")
invalid_topic = app.topic(INVALID_TOPIC_NAME, value_serializer="json")

sdf = app.dataframe(topic=input_topic)

valid_sdf = sdf.filter(is_valid)
invalid_sdf = sdf.filter(lambda value: not is_valid(value)).apply(to_invalid_record)
invalid_sdf.update(persist_invalid)
invalid_sdf.to_topic(invalid_topic)

windowed_sdf = (
    valid_sdf.group_by("service_name")
    .hopping_window(
        duration_ms=timedelta(seconds=WINDOW_SIZE_SEC),
        step_ms=timedelta(seconds=WINDOW_STEP_SEC),
        grace_ms=timedelta(seconds=LATE_GRACE_SEC),
    )
    .agg(metrics=TokenMetricsAggregator())
    .current()
    .apply(to_window_record, metadata=True)
)

windowed_sdf.update(persist_window)
windowed_sdf.to_topic(analytics_topic)

alerts_sdf = windowed_sdf.filter(is_alert).apply(to_alert_record)
alerts_sdf.update(persist_alert)
alerts_sdf.to_topic(alerts_topic)

app.run()
