CREATE TABLE IF NOT EXISTS legacy_report_archive (
  import_key CHAR(64) PRIMARY KEY,
  source_path VARCHAR(600) NOT NULL,
  source_sha256 CHAR(64) NOT NULL,
  source_bytes BIGINT UNSIGNED NOT NULL,
  archive_status VARCHAR(32) NOT NULL,
  provenance JSON NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS legacy_report_alias (
  alias VARCHAR(120) COLLATE utf8mb4_bin NOT NULL,
  import_key CHAR(64) NOT NULL,
  alias_kind VARCHAR(32) NOT NULL,
  PRIMARY KEY (alias,import_key,alias_kind),
  FOREIGN KEY (import_key) REFERENCES legacy_report_archive(import_key)
) ENGINE=InnoDB;
