import json
import os
from collections import Counter

from pymongo import MongoClient

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:example@localhost:27017')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'streamanalytics')

client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]

analytics = list(db.analytics_windows.find({}, {'_id': 0}))
alerts = list(db.alerts.find({}, {'_id': 0}))
invalid = list(db.invalid_records.find({}, {'_id': 0}))

print('=== Result summary ===')
print(f'analytics_windows={len(analytics)}')
print(f'alerts={len(alerts)}')
print(f'invalid_records={len(invalid)}')

if analytics:
    avg_throughput = sum(doc.get('throughput_rps', 0.0) for doc in analytics) / len(analytics)
    avg_latency = sum(doc.get('avg_processing_latency_ms', 0.0) for doc in analytics) / len(analytics)
    top_services = Counter(doc.get('service_name', 'unknown') for doc in analytics).most_common(5)
    print(f'avg_throughput_rps={avg_throughput:.3f}')
    print(f'avg_processing_latency_ms={avg_latency:.3f}')
    print('top_services=' + json.dumps(top_services))

if alerts:
    top_alert_types = Counter(doc.get('alert_type', 'unknown') for doc in alerts).most_common(5)
    print('top_alert_types=' + json.dumps(top_alert_types))

if invalid:
    top_invalid_reasons = Counter(doc.get('reason', 'unknown') for doc in invalid).most_common(5)
    print('top_invalid_reasons=' + json.dumps(top_invalid_reasons))
