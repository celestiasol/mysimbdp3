const dbName = process.env.MONGO_INITDB_DATABASE || 'streamanalytics';
const dbRef = db.getSiblingDB(dbName);

dbRef.createCollection('analytics_windows');
dbRef.createCollection('alerts');
dbRef.createCollection('invalid_records');

dbRef.analytics_windows.createIndex(
  { service_name: 1, window_start_ms: 1, window_end_ms: 1 },
  { unique: true }
);
dbRef.analytics_windows.createIndex({ window_id: 1 }, { unique: true });
dbRef.analytics_windows.createIndex({ updated_at_ms: 1 });

dbRef.alerts.createIndex({ alert_id: 1 }, { unique: true });
dbRef.alerts.createIndex({ service_name: 1, window_start_ms: 1, window_end_ms: 1, alert_type: 1 });

dbRef.invalid_records.createIndex({ event_id: 1, reason: 1 }, { unique: true });
dbRef.invalid_records.createIndex({ received_at_ms: 1 });
