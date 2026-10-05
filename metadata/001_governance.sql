CREATE TABLE IF NOT EXISTS schema_migration (
  version INT PRIMARY KEY, applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dataset (
  dataset_id VARCHAR(80) PRIMARY KEY, description VARCHAR(255) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dataset_version (
  version_id VARCHAR(100) PRIMARY KEY,
  dataset_id VARCHAR(80) NOT NULL,
  parent_version_id VARCHAR(100),
  content_hash CHAR(64) NOT NULL,
  manifest JSON NOT NULL,
  status VARCHAR(24) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
  FOREIGN KEY (parent_version_id) REFERENCES dataset_version(version_id),
  INDEX dataset_versions (dataset_id, created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS definition_version (
  kind VARCHAR(16) NOT NULL, version_id VARCHAR(80) NOT NULL,
  content_hash CHAR(64) NOT NULL, definition JSON NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (kind, version_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dataset_location (
  version_id VARCHAR(100) NOT NULL, source_table VARCHAR(24) NOT NULL,
  storage_uri VARCHAR(600) NOT NULL, sha256 CHAR(64) NOT NULL, bytes BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (version_id,source_table),
  FOREIGN KEY (version_id) REFERENCES dataset_version(version_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS logical_run (
  request_seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
  run_id VARCHAR(80) PRIMARY KEY,
  dataset_id VARCHAR(80) NOT NULL,
  input_version VARCHAR(100) NOT NULL,
  rule_version VARCHAR(80) NOT NULL, metric_version VARCHAR(80) NOT NULL,
  idempotency_scope VARCHAR(80) NOT NULL,
  idempotency_key VARCHAR(120) NOT NULL,
  request_hash CHAR(64) NOT NULL, request JSON NOT NULL,
  status VARCHAR(24) NOT NULL, stage VARCHAR(80) NOT NULL,
  active_attempt VARCHAR(80), fencing_token BIGINT UNSIGNED NOT NULL DEFAULT 0,
  lease_owner VARCHAR(80), lease_until DATETIME(6),
  error JSON, created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY request_identity (idempotency_scope, idempotency_key),
  FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
  FOREIGN KEY (input_version) REFERENCES dataset_version(version_id),
  INDEX run_queue (status, request_seq),
  INDEX run_history (dataset_id, status, created_at),
  INDEX version_comparison (input_version, rule_version, metric_version)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS physical_attempt (
  attempt_id VARCHAR(80) PRIMARY KEY, run_id VARCHAR(80) NOT NULL,
  attempt_no INT UNSIGNED NOT NULL, fencing_token BIGINT UNSIGNED NOT NULL,
  status VARCHAR(24) NOT NULL, stage VARCHAR(80) NOT NULL,
  work_path VARCHAR(600) NOT NULL, environment JSON NOT NULL,
  started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ended_at DATETIME(6), error JSON,
  UNIQUE KEY attempt_number (run_id, attempt_no),
  FOREIGN KEY (run_id) REFERENCES logical_run(run_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS hadoop_job (
  submission_id VARCHAR(100) PRIMARY KEY, attempt_id VARCHAR(80) NOT NULL,
  stage VARCHAR(80) NOT NULL, job_name VARCHAR(120) NOT NULL,
  job_id VARCHAR(80), application_id VARCHAR(80),
  status VARCHAR(24) NOT NULL, specification JSON NOT NULL, detail JSON,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY attempt_stage (attempt_id, stage),
  UNIQUE KEY application_identity (application_id),
  FOREIGN KEY (attempt_id) REFERENCES physical_attempt(attempt_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS quality_result (
  attempt_id VARCHAR(80) NOT NULL, phase VARCHAR(12) NOT NULL,
  source_table VARCHAR(24) NOT NULL, metric VARCHAR(24) NOT NULL,
  metric_version VARCHAR(80) NOT NULL,
  numerator BIGINT, denominator BIGINT, score DECIMAL(12,6),
  detail JSON NOT NULL,
  PRIMARY KEY (attempt_id, phase, source_table, metric),
  FOREIGN KEY (attempt_id) REFERENCES physical_attempt(attempt_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS evidence_index (
  evidence_id CHAR(64) PRIMARY KEY, attempt_id VARCHAR(80) NOT NULL,
  source_record_id CHAR(64) NOT NULL, source_table VARCHAR(24) NOT NULL,
  rule_id VARCHAR(80) NOT NULL, metric VARCHAR(40),
  file_path VARCHAR(600) NOT NULL, row_group INT NOT NULL, row_in_group BIGINT NOT NULL,
  detail JSON NOT NULL,
  FOREIGN KEY (attempt_id) REFERENCES physical_attempt(attempt_id),
  INDEX evidence_source (attempt_id, source_record_id),
  INDEX evidence_rule (attempt_id, rule_id),
  INDEX evidence_metric (attempt_id, metric),
  UNIQUE KEY evidence_position (attempt_id, file_path, row_group, row_in_group)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS publish_version (
  publish_id VARCHAR(80) PRIMARY KEY, run_id VARCHAR(80) NOT NULL UNIQUE,
  attempt_id VARCHAR(80) NOT NULL, output_version VARCHAR(100) NOT NULL UNIQUE,
  fencing_token BIGINT UNSIGNED NOT NULL,
  status VARCHAR(24) NOT NULL, manifest JSON NOT NULL, manifest_hash CHAR(64) NOT NULL,
  storage_path VARCHAR(600) NOT NULL, report_path VARCHAR(600) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), confirmed_at DATETIME(6),
  FOREIGN KEY (run_id) REFERENCES logical_run(run_id),
  FOREIGN KEY (attempt_id) REFERENCES physical_attempt(attempt_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS dataset_current (
  dataset_id VARCHAR(80) PRIMARY KEY, publish_id VARCHAR(80) NOT NULL,
  request_seq BIGINT UNSIGNED NOT NULL, revision BIGINT UNSIGNED NOT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY (dataset_id) REFERENCES dataset(dataset_id),
  FOREIGN KEY (publish_id) REFERENCES publish_version(publish_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS run_event (
  event_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  run_id VARCHAR(80) NOT NULL, attempt_id VARCHAR(80),
  event_type VARCHAR(80) NOT NULL, detail JSON NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY (run_id) REFERENCES logical_run(run_id),
  INDEX run_events (run_id, event_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS worker_slot (
  slot_id INT PRIMARY KEY, run_id VARCHAR(80), lease_owner VARCHAR(80), lease_until DATETIME(6)
) ENGINE=InnoDB;
INSERT IGNORE INTO worker_slot (slot_id) VALUES (1);
INSERT IGNORE INTO schema_migration (version) VALUES (1);
CREATE TABLE IF NOT EXISTS runtime_control (
  control_id INT PRIMARY KEY, admission_open BOOLEAN NOT NULL,
  revision BIGINT UNSIGNED NOT NULL, owner VARCHAR(80), reason VARCHAR(1000), switch_id CHAR(32),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
INSERT IGNORE INTO runtime_control (control_id,admission_open,revision) VALUES (1,TRUE,0);
CREATE TABLE IF NOT EXISTS runtime_control_event (
  revision BIGINT UNSIGNED PRIMARY KEY, admission_open BOOLEAN NOT NULL,
  owner VARCHAR(80) NOT NULL, reason VARCHAR(1000) NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
INSERT IGNORE INTO schema_migration (version) VALUES (5);
