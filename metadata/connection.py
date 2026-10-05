"""MySQL connection and idempotent schema setup for the dedicated project database."""
import contextlib
import os
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[1]


class MetadataUnavailable(RuntimeError):
    """Required database configuration is unavailable."""


def load_config():
    values = {}
    path = ROOT / "runtime" / ".env"
    if path.is_file():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
    values.update({key: value for key, value in os.environ.items() if key.startswith("ML_MYSQL_")})
    if not values.get("ML_MYSQL_PASSWORD"):
        raise MetadataUnavailable("MySQL credentials missing; run runtime/prepare-governance.ps1")
    return {"host": values.get("ML_MYSQL_HOST", "127.0.0.1"),
            "port": int(values.get("ML_MYSQL_PORT", "13306")),
            "user": values.get("ML_MYSQL_USER", "ml_governance"),
            "password": values["ML_MYSQL_PASSWORD"],
            "database": values.get("ML_MYSQL_DATABASE", "ml_governance")}


def connect(config=None):
    return pymysql.connect(**(config or load_config()), charset="utf8mb4",
                           cursorclass=pymysql.cursors.DictCursor, autocommit=False,
                           connect_timeout=10, read_timeout=30, write_timeout=30)


@contextlib.contextmanager
def transaction(config=None):
    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def migrate(config=None):
    """Only additive, restartable DDL; never drop a database or existing table."""
    sql = (Path(__file__).parent / "001_governance.sql").read_text(encoding="utf-8")
    with transaction(config) as cursor:
        cursor.execute("SELECT GET_LOCK('ml_governance_schema_v1', 30) AS acquired")
        if cursor.fetchone()["acquired"] != 1:
            raise RuntimeError("Another schema migration is running")
        try:
            for statement in sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)
            cursor.execute("SELECT COUNT(*) AS n FROM information_schema.columns WHERE table_schema=DATABASE() "
                           "AND table_name='runtime_control' AND column_name='switch_id'")
            if cursor.fetchone()['n']==0:
                cursor.execute('ALTER TABLE runtime_control ADD COLUMN switch_id CHAR(32) NULL')
            cursor.execute("SELECT COUNT(*) AS n FROM information_schema.statistics "
                           "WHERE table_schema=DATABASE() AND table_name='evidence_index' AND index_name='evidence_position'")
            if cursor.fetchone()["n"] == 0:
                cursor.execute("ALTER TABLE evidence_index ADD UNIQUE KEY evidence_position "
                               "(attempt_id,file_path,row_group,row_in_group)")
            for name, columns in (("evidence_page", "attempt_id,evidence_id"),
                                  ("evidence_table_page", "attempt_id,source_table,evidence_id")):
                cursor.execute("SELECT COUNT(*) AS n FROM information_schema.statistics "
                               "WHERE table_schema=DATABASE() AND table_name='evidence_index' AND index_name=%s", (name,))
                if cursor.fetchone()["n"] == 0:
                    cursor.execute(f"ALTER TABLE evidence_index ADD INDEX {name} ({columns})")
            cursor.execute("INSERT IGNORE INTO schema_migration (version) VALUES (4)")
            cursor.execute("INSERT IGNORE INTO schema_migration (version) VALUES (2)")
        finally:
            cursor.execute("SELECT RELEASE_LOCK('ml_governance_schema_v1')")


if __name__ == "__main__":
    migrate()
    print("MySQL governance schema version 5 ready (legacy archive has separate migration)")
