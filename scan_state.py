import argparse
from collections import Counter, defaultdict
import csv
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock


SQLITE_CORRUPTION_MARKERS = (
    "database disk image is malformed",
    "file is not a database",
    "malformed database schema",
    "database corrupt",
    "rowid out of order",
    "missing from index",
    "wrong # of entries in index",
)


class ScanStateStore:
    def __init__(self, db_path, market, log_date_format="%Y-%m-%d"):
        self.db_path = str(db_path)
        self.market = market
        self.log_date_format = log_date_format
        self.lock = RLock()
        self.conn = None
        with self.lock:
            self._open_and_prepare_locked(initializing=True)

    def _log(self, message):
        print(f"⚠ ScanStateStore[{self.market}] {message}", file=sys.stderr)

    def _open_connection_locked(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _close_connection_locked(self):
        if self.conn is None:
            return
        try:
            try:
                self.conn.execute("PRAGMA optimize")
            except sqlite3.DatabaseError:
                pass
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.DatabaseError:
                pass
        finally:
            self.conn.close()
            self.conn = None

    def _db_related_paths(self, db_path=None):
        base = Path(db_path or self.db_path)
        return [base, Path(f"{base}-wal"), Path(f"{base}-shm")]

    def _remove_db_files_locked(self, db_path=None):
        for candidate in self._db_related_paths(db_path=db_path):
            if candidate.exists():
                candidate.unlink()

    def _is_corruption_error(self, exc):
        if not isinstance(exc, sqlite3.DatabaseError):
            return False
        message = str(exc).lower()
        return any(marker in message for marker in SQLITE_CORRUPTION_MARKERS)

    def _validate_connection_locked(self, pragma="quick_check"):
        rows = self.conn.execute(f"PRAGMA {pragma}").fetchall()
        messages = [str(row[0]) for row in rows if row and row[0] is not None]
        if not messages or messages == ["ok"]:
            return
        raise sqlite3.DatabaseError("; ".join(messages[:10]))

    def _recover_from_backup_locked(self, source_db, target_db):
        try:
            recovered = subprocess.run(
                ["sqlite3", str(source_db), ".recover --ignore-freelist"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return False
        if recovered.returncode != 0 or not recovered.stdout.strip():
            return False
        imported = subprocess.run(
            ["sqlite3", str(target_db)],
            input=recovered.stdout,
            capture_output=True,
            text=True,
            check=False,
        )
        return imported.returncode == 0

    def _recover_database_locked(self, reason):
        self._close_connection_locked()

        base = Path(self.db_path)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantined_db = base.with_name(f"{base.name}.corrupt-{stamp}")
        for source, target in zip(self._db_related_paths(), self._db_related_paths(quarantined_db)):
            if source.exists():
                os.replace(source, target)

        restored = quarantined_db.exists() and self._recover_from_backup_locked(quarantined_db, base)
        if restored:
            self._log(f"Recovered {base.name} after {reason}. Corrupt copy kept at {quarantined_db}")
        else:
            self._remove_db_files_locked(db_path=base)
            self._log(f"Recreated clean {base.name} after {reason}. Corrupt copy kept at {quarantined_db}")

        self._open_connection_locked()
        self._configure()
        self._init_schema()
        try:
            self._validate_connection_locked(pragma="quick_check")
        except sqlite3.DatabaseError as exc:
            self._close_connection_locked()
            self._remove_db_files_locked(db_path=base)
            self._open_connection_locked()
            self._configure()
            self._init_schema()
            self._log(
                f"Recovered database did not pass health check ({exc}); started with a clean {base.name}"
            )

    def _open_and_prepare_locked(self, initializing=False):
        self._open_connection_locked()
        try:
            self._configure()
            self._validate_connection_locked(pragma="quick_check")
            self._init_schema()
        except sqlite3.DatabaseError as exc:
            if not self._is_corruption_error(exc):
                self._close_connection_locked()
                raise
            phase = "startup" if initializing else "reconnect"
            self._log(f"{phase} integrity failure in {self.db_path}: {exc}")
            self._recover_database_locked(reason=f"{phase}: {exc}")

    def _ensure_connection_locked(self):
        if self.conn is None:
            self._open_and_prepare_locked(initializing=False)

    def _run_db_operation(self, action, callback):
        with self.lock:
            self._ensure_connection_locked()
            try:
                return callback()
            except sqlite3.DatabaseError as exc:
                if not self._is_corruption_error(exc):
                    raise
                self._log(f"{action} hit corruption in {self.db_path}: {exc}")
                self._recover_database_locked(reason=f"{action}: {exc}")
                return callback()

    def _resolve_run_id_locked(self, run_id):
        if run_id is None:
            return None
        row = self.conn.execute(
            "SELECT 1 FROM scan_runs WHERE id = ? AND market = ? LIMIT 1",
            (run_id, self.market),
        ).fetchone()
        return run_id if row is not None else None

    def _configure(self):
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.execute("PRAGMA wal_autocheckpoint=1000")

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL,
                    run_date TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    total_scanned INTEGER,
                    total_results INTEGER
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS latest_scan_state (
                    market TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    score INTEGER NOT NULL DEFAULT 0,
                    failed TEXT NOT NULL DEFAULT '',
                    scan_date TEXT,
                    scanned_at TEXT NOT NULL,
                    payload_json TEXT,
                    run_id INTEGER,
                    source TEXT NOT NULL DEFAULT 'runtime',
                    PRIMARY KEY (market, ticker),
                    FOREIGN KEY(run_id) REFERENCES scan_runs(id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    score INTEGER NOT NULL DEFAULT 0,
                    failed TEXT NOT NULL DEFAULT '',
                    scan_date TEXT,
                    scanned_at TEXT NOT NULL,
                    payload_json TEXT,
                    run_id INTEGER,
                    source TEXT NOT NULL DEFAULT 'runtime',
                    FOREIGN KEY(run_id) REFERENCES scan_runs(id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    run_id INTEGER,
                    UNIQUE(market, snapshot_date),
                    FOREIGN KEY(run_id) REFERENCES scan_runs(id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scan_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    scan_generated_at TEXT,
                    source_file TEXT,
                    reference_file TEXT,
                    reference_date TEXT,
                    payload_json TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_scan_runs_market_started
                ON scan_runs (market, started_at)
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_latest_scan_state_market_date
                ON latest_scan_state (market, scan_date)
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_scan_history_market_ticker
                ON scan_history (market, ticker, scanned_at)
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_scan_artifacts_market_type_generated
                ON scan_artifacts (market, artifact_type, generated_at)
                """
            )
            for table_name in ("us_command_archive", "india_command_archive", "cross_market_command_archive"):
                self.conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        command_name TEXT NOT NULL,
                        generated_at TEXT NOT NULL,
                        scan_generated_at TEXT,
                        source_file TEXT,
                        reference_file TEXT,
                        reference_date TEXT,
                        payload_json TEXT NOT NULL,
                        legacy_artifact_id INTEGER
                    )
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_type_generated
                    ON {table_name} (command_name, generated_at)
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_legacy
                    ON {table_name} (legacy_artifact_id)
                    WHERE legacy_artifact_id IS NOT NULL
                    """
                )
            for table_name in ("us_report_history", "india_report_history"):
                self.conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        generated_at TEXT NOT NULL,
                        scan_generated_at TEXT,
                        source_file TEXT,
                        total_scanned INTEGER,
                        total_results INTEGER,
                        avg_score REAL,
                        top_score INTEGER,
                        score_distribution_json TEXT,
                        signal_summary_json TEXT,
                        sector_leaders_json TEXT,
                        payload_json TEXT NOT NULL,
                        command_archive_id INTEGER
                    )
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_scan_generated
                    ON {table_name} (scan_generated_at, generated_at)
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_command_archive
                    ON {table_name} (command_archive_id)
                    WHERE command_archive_id IS NOT NULL
                    """
                )
            for table_name in ("us_sector_report_history", "india_sector_report_history"):
                self.conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        generated_at TEXT NOT NULL,
                        scan_generated_at TEXT,
                        source_file TEXT,
                        requested_sector TEXT,
                        sector TEXT NOT NULL,
                        sector_rank INTEGER,
                        sector_count INTEGER,
                        avg_score REAL,
                        best_score INTEGER,
                        near_high_count INTEGER,
                        avg_turnover REAL,
                        leader TEXT,
                        total_results INTEGER,
                        filtered_count INTEGER,
                        payload_json TEXT NOT NULL,
                        command_archive_id INTEGER
                    )
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_sector_scan
                    ON {table_name} (sector, scan_generated_at)
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_requested_generated
                    ON {table_name} (requested_sector, generated_at)
                    """
                )
                self.conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_command_sector
                    ON {table_name} (command_archive_id, sector)
                    WHERE command_archive_id IS NOT NULL
                    """
                )
        self._ensure_column("latest_scan_state", "payload_json", "TEXT")
        self._ensure_column("scan_history", "payload_json", "TEXT")
        self._backfill_command_archives()
        self._backfill_special_histories()

    def _command_archive_table(self, market=None):
        scope_market = (market or self.market or "").upper()
        mapping = {
            "US": "us_command_archive",
            "IN": "india_command_archive",
            "ALL": "cross_market_command_archive",
        }
        return mapping.get(scope_market)

    def _report_history_table(self, market=None):
        scope_market = (market or self.market or "").upper()
        mapping = {
            "US": "us_report_history",
            "IN": "india_report_history",
        }
        return mapping.get(scope_market)

    def _sector_history_table(self, market=None):
        scope_market = (market or self.market or "").upper()
        mapping = {
            "US": "us_sector_report_history",
            "IN": "india_sector_report_history",
        }
        return mapping.get(scope_market)

    def _backfill_command_archives(self):
        table_names = ("us_command_archive", "india_command_archive", "cross_market_command_archive")
        with self.lock:
            existing_rows = 0
            for table_name in table_names:
                existing_rows += self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if existing_rows:
                return

            rows = self.conn.execute(
                """
                SELECT id, market, artifact_type, generated_at, scan_generated_at, source_file, reference_file, reference_date, payload_json
                FROM scan_artifacts
                ORDER BY id ASC
                """
            ).fetchall()
            if not rows:
                return

            for row in rows:
                table_name = self._command_archive_table(row["market"])
                if not table_name:
                    continue
                self.conn.execute(
                    f"""
                    INSERT OR IGNORE INTO {table_name} (
                        command_name, generated_at, scan_generated_at,
                        source_file, reference_file, reference_date, payload_json, legacy_artifact_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["artifact_type"],
                        row["generated_at"],
                        row["scan_generated_at"],
                        row["source_file"],
                        row["reference_file"],
                        row["reference_date"],
                        row["payload_json"],
                        row["id"],
                    ),
                )
            self.conn.commit()

    def _backfill_special_histories(self):
        with self.lock:
            for market, archive_table, report_table, sector_table in (
                ("US", "us_command_archive", "us_report_history", "us_sector_report_history"),
                ("IN", "india_command_archive", "india_report_history", "india_sector_report_history"),
            ):
                report_rows = self.conn.execute(
                    f"""
                    SELECT id, generated_at, scan_generated_at, source_file, payload_json
                    FROM {archive_table}
                    WHERE command_name = 'report'
                      AND id NOT IN (
                          SELECT command_archive_id FROM {report_table} WHERE command_archive_id IS NOT NULL
                      )
                    ORDER BY id ASC
                    """
                ).fetchall()
                for row in report_rows:
                    payload = _deserialize_payload(row["payload_json"])
                    if isinstance(payload, dict):
                        self._insert_report_history_row(
                            market=market,
                            payload=payload,
                            source_file=row["source_file"],
                            command_archive_id=row["id"],
                            generated_at=row["generated_at"],
                            scan_generated_at=row["scan_generated_at"],
                        )

                sector_rows = self.conn.execute(
                    f"""
                    SELECT id, generated_at, scan_generated_at, source_file, payload_json
                    FROM {archive_table}
                    WHERE command_name = 'sector-report'
                      AND id NOT IN (
                          SELECT command_archive_id FROM {sector_table} WHERE command_archive_id IS NOT NULL
                      )
                    ORDER BY id ASC
                    """
                ).fetchall()
                for row in sector_rows:
                    payload = _deserialize_payload(row["payload_json"])
                    if isinstance(payload, dict):
                        self._insert_sector_history_rows(
                            market=market,
                            payload=payload,
                            source_file=row["source_file"],
                            command_archive_id=row["id"],
                            generated_at=row["generated_at"],
                            scan_generated_at=row["scan_generated_at"],
                        )
            self.conn.commit()

    def _column_exists(self, table_name, column_name):
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(row["name"] == column_name for row in rows)

    def _ensure_column(self, table_name, column_name, definition):
        if self._column_exists(table_name, column_name):
            return
        with self.conn:
            self.conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

    def close(self):
        with self.lock:
            self._close_connection_locked()

    def parse_scan_date(self, value):
        if not value:
            return None
        try:
            return datetime.strptime(value, self.log_date_format).date()
        except Exception:
            return None

    def parse_scan_entry(self, line):
        line = line.strip()
        if not line:
            return None

        parts = [part.strip() for part in line.split(",")]
        ticker = parts[0]
        if not ticker:
            return None

        entry = {"ticker": ticker, "score": 0, "failed": "", "date": None}
        for part in parts[1:]:
            key, sep, value = part.partition("=")
            if not sep:
                continue
            key = key.strip().lower()
            value = value.strip()
            if key == "score":
                try:
                    entry["score"] = int(value)
                except Exception:
                    entry["score"] = 0
            elif key == "failed":
                entry["failed"] = value
            elif key == "date":
                entry["date"] = self.parse_scan_date(value)
        return entry

    def _scan_date_to_text(self, scan_date):
        if scan_date is None:
            return None
        if hasattr(scan_date, "strftime"):
            return scan_date.strftime(self.log_date_format)
        return str(scan_date)

    def has_market_state(self):
        def op():
            row = self.conn.execute(
                "SELECT 1 FROM latest_scan_state WHERE market = ? LIMIT 1",
                (self.market,),
            ).fetchone()
            return row is not None

        return self._run_db_operation("has_market_state", op)

    def load_latest_state(self):
        def op():
            rows = self.conn.execute(
                """
                SELECT ticker, score, failed, scan_date
                FROM latest_scan_state
                WHERE market = ?
                """,
                (self.market,),
            ).fetchall()
            return rows

        rows = self._run_db_operation("load_latest_state", op)
        history = {}
        for row in rows:
            history[row["ticker"]] = {
                "ticker": row["ticker"],
                "score": int(row["score"] or 0),
                "failed": row["failed"] or "",
                "date": self.parse_scan_date(row["scan_date"]),
            }
        return history

    def has_market_history(self):
        def op():
            row = self.conn.execute(
                "SELECT 1 FROM scan_history WHERE market = ? LIMIT 1",
                (self.market,),
            ).fetchone()
            return row is not None

        return self._run_db_operation("has_market_history", op)

    def load_history_rows(self):
        def op():
            rows = self.conn.execute(
                """
                SELECT ticker, score, failed, scan_date, scanned_at, payload_json, source
                FROM scan_history
                WHERE market = ?
                ORDER BY scanned_at, id
                """,
                (self.market,),
            ).fetchall()
            return [dict(row) for row in rows]

        return self._run_db_operation("load_history_rows", op)

    def summarize_market_history(self, event_limit=10, run_limit=5, snapshot_limit=5, ticker_limit=10):
        def op():
            totals = self.conn.execute(
                """
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT ticker) AS unique_tickers,
                    MIN(scan_date) AS first_scan_date,
                    MAX(scan_date) AS last_scan_date,
                    MAX(scanned_at) AS last_event_at
                FROM scan_history
                WHERE market = ?
                """,
                (self.market,),
            ).fetchone()
            recent_runs = self.conn.execute(
                """
                SELECT run_date, started_at, completed_at, status, total_scanned, total_results
                FROM scan_runs
                WHERE market = ?
                ORDER BY started_at DESC, id DESC
                LIMIT ?
                """,
                (self.market, run_limit),
            ).fetchall()
            recent_events = self.conn.execute(
                """
                SELECT ticker, score, failed, scan_date, scanned_at, source
                FROM scan_history
                WHERE market = ?
                ORDER BY scanned_at DESC, id DESC
                LIMIT ?
                """,
                (self.market, event_limit),
            ).fetchall()
            top_tickers = self.conn.execute(
                """
                SELECT ticker, COUNT(*) AS scans, MAX(scan_date) AS last_scan_date, MAX(score) AS best_score
                FROM scan_history
                WHERE market = ?
                GROUP BY ticker
                ORDER BY scans DESC, ticker ASC
                LIMIT ?
                """,
                (self.market, ticker_limit),
            ).fetchall()
            recent_snapshots = self.conn.execute(
                """
                SELECT snapshot_date, file_path, created_at
                FROM scan_snapshots
                WHERE market = ?
                ORDER BY snapshot_date DESC, id DESC
                LIMIT ?
                """,
                (self.market, snapshot_limit),
            ).fetchall()
            return totals, recent_runs, recent_events, top_tickers, recent_snapshots

        totals, recent_runs, recent_events, top_tickers, recent_snapshots = self._run_db_operation(
            "summarize_market_history",
            op,
        )
        return {
            "market": self.market,
            "db_path": self.db_path,
            "summary": {
                "total_events": totals["total_events"] or 0,
                "unique_tickers": totals["unique_tickers"] or 0,
                "first_scan_date": totals["first_scan_date"],
                "last_scan_date": totals["last_scan_date"],
                "last_event_at": totals["last_event_at"],
            },
            "recent_runs": [dict(row) for row in recent_runs],
            "recent_events": [dict(row) for row in recent_events],
            "most_scanned_tickers": [dict(row) for row in top_tickers],
            "recent_snapshots": [dict(row) for row in recent_snapshots],
        }

    def _serialize_payload(self, payload):
        if payload is None:
            return None
        try:
            return json.dumps(payload, ensure_ascii=True, default=str)
        except Exception:
            return json.dumps({"repr": repr(payload)}, ensure_ascii=True)

    def start_run(self, run_date):
        started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        def op():
            cursor = self.conn.execute(
                """
                INSERT INTO scan_runs (market, run_date, started_at, status)
                VALUES (?, ?, ?, 'running')
                """,
                (self.market, run_date, started_at),
            )
            self.conn.commit()
            return cursor.lastrowid

        return self._run_db_operation("start_run", op)

    def finish_run(self, run_id, status="completed", total_scanned=None, total_results=None):
        completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        def op():
            resolved_run_id = self._resolve_run_id_locked(run_id)
            if resolved_run_id is None:
                return False
            cursor = self.conn.execute(
                """
                UPDATE scan_runs
                SET completed_at = ?,
                    status = ?,
                    total_scanned = ?,
                    total_results = ?
                WHERE id = ?
                """,
                (completed_at, status, total_scanned, total_results, resolved_run_id),
            )
            self.conn.commit()
            return cursor.rowcount > 0

        return self._run_db_operation("finish_run", op)

    def record_scan(self, ticker, score=0, failed="", run_id=None, scan_date=None, scanned_at=None, source="runtime", payload=None):
        scan_date_text = self._scan_date_to_text(scan_date)
        scanned_at_text = scanned_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload_json = self._serialize_payload(payload)

        def op():
            resolved_run_id = self._resolve_run_id_locked(run_id)
            self.conn.execute(
                """
                INSERT INTO scan_history (
                    market, ticker, score, failed, scan_date, scanned_at, payload_json, run_id, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.market,
                    ticker,
                    score,
                    failed or "",
                    scan_date_text,
                    scanned_at_text,
                    payload_json,
                    resolved_run_id,
                    source,
                ),
            )
            self.conn.execute(
                """
                INSERT INTO latest_scan_state (
                    market, ticker, score, failed, scan_date, scanned_at, payload_json, run_id, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market, ticker) DO UPDATE SET
                    score = excluded.score,
                    failed = excluded.failed,
                    scan_date = excluded.scan_date,
                    scanned_at = excluded.scanned_at,
                    payload_json = excluded.payload_json,
                    run_id = excluded.run_id,
                    source = excluded.source
                """,
                (
                    self.market,
                    ticker,
                    score,
                    failed or "",
                    scan_date_text,
                    scanned_at_text,
                    payload_json,
                    resolved_run_id,
                    source,
                ),
            )
            self.conn.commit()

        self._run_db_operation("record_scan", op)

    def record_snapshot(self, snapshot_date, file_path, run_id=None):
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        def op():
            resolved_run_id = self._resolve_run_id_locked(run_id)
            self.conn.execute(
                """
                INSERT INTO scan_snapshots (market, snapshot_date, file_path, created_at, run_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(market, snapshot_date) DO UPDATE SET
                    file_path = excluded.file_path,
                    created_at = excluded.created_at,
                    run_id = excluded.run_id
                """,
                (self.market, snapshot_date, file_path, created_at, resolved_run_id),
            )
            self.conn.commit()

        self._run_db_operation("record_snapshot", op)

    def record_artifact(self, artifact_type, payload, scan_generated_at=None, source_file=None, reference_file=None, reference_date=None):
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload_json = self._serialize_payload(payload) or "{}"
        def op():
            if artifact_type == "diff":
                existing = self.conn.execute(
                    """
                    SELECT id
                    FROM scan_artifacts
                    WHERE market = ?
                      AND artifact_type = ?
                      AND COALESCE(scan_generated_at, '') = COALESCE(?, '')
                      AND COALESCE(reference_date, '') = COALESCE(?, '')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (self.market, artifact_type, scan_generated_at, reference_date),
                ).fetchone()
                if existing is not None:
                    self.conn.execute(
                        """
                        UPDATE scan_artifacts
                        SET generated_at = ?,
                            source_file = ?,
                            reference_file = ?,
                            reference_date = ?,
                            payload_json = ?
                        WHERE id = ?
                        """,
                        (
                            generated_at,
                            source_file,
                            reference_file,
                            reference_date,
                            payload_json,
                            existing["id"],
                        ),
                    )
                    self.conn.execute(
                        """
                        DELETE FROM scan_artifacts
                        WHERE market = ?
                          AND artifact_type = ?
                          AND COALESCE(scan_generated_at, '') = COALESCE(?, '')
                          AND COALESCE(reference_date, '') = COALESCE(?, '')
                          AND id != ?
                        """,
                        (self.market, artifact_type, scan_generated_at, reference_date, existing["id"]),
                    )
                    self.conn.commit()
                    return existing["id"]
            cursor = self.conn.execute(
                """
                INSERT INTO scan_artifacts (
                    market, artifact_type, generated_at, scan_generated_at,
                    source_file, reference_file, reference_date, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.market,
                    artifact_type,
                    generated_at,
                    scan_generated_at,
                    source_file,
                    reference_file,
                    reference_date,
                    payload_json,
                ),
            )
            self.conn.commit()
            return cursor.lastrowid

        return self._run_db_operation("record_artifact", op)

    def record_command_output(self, command_name, payload, market=None, scan_generated_at=None, source_file=None, reference_file=None, reference_date=None, legacy_artifact_id=None):
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload_json = self._serialize_payload(payload) or "{}"
        table_name = self._command_archive_table(market=market)
        if not table_name:
            return None
        def op():
            if command_name == "diff":
                existing = self.conn.execute(
                    f"""
                    SELECT id
                    FROM {table_name}
                    WHERE command_name = ?
                      AND COALESCE(scan_generated_at, '') = COALESCE(?, '')
                      AND COALESCE(reference_date, '') = COALESCE(?, '')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (command_name, scan_generated_at, reference_date),
                ).fetchone()
                if existing is not None:
                    self.conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET generated_at = ?,
                            source_file = ?,
                            reference_file = ?,
                            reference_date = ?,
                            payload_json = ?,
                            legacy_artifact_id = COALESCE(?, legacy_artifact_id)
                        WHERE id = ?
                        """,
                        (
                            generated_at,
                            source_file,
                            reference_file,
                            reference_date,
                            payload_json,
                            legacy_artifact_id,
                            existing["id"],
                        ),
                    )
                    self.conn.execute(
                        f"""
                        DELETE FROM {table_name}
                        WHERE command_name = ?
                          AND COALESCE(scan_generated_at, '') = COALESCE(?, '')
                          AND COALESCE(reference_date, '') = COALESCE(?, '')
                          AND id != ?
                        """,
                        (command_name, scan_generated_at, reference_date, existing["id"]),
                    )
                    self.conn.commit()
                    return existing["id"]
            cursor = self.conn.execute(
                f"""
                INSERT INTO {table_name} (
                    command_name, generated_at, scan_generated_at,
                    source_file, reference_file, reference_date, payload_json, legacy_artifact_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command_name,
                    generated_at,
                    scan_generated_at,
                    source_file,
                    reference_file,
                    reference_date,
                    payload_json,
                    legacy_artifact_id,
                ),
            )
            self.conn.commit()
            return cursor.lastrowid

        return self._run_db_operation("record_command_output", op)

    def _insert_report_history_row(self, market, payload, source_file=None, command_archive_id=None, generated_at=None, scan_generated_at=None):
        table_name = self._report_history_table(market=market)
        if not table_name or not isinstance(payload, dict):
            return None
        generated_text = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scan_generated_text = scan_generated_at or payload.get("generated_at")
        cursor = self.conn.execute(
            f"""
            INSERT OR IGNORE INTO {table_name} (
                generated_at, scan_generated_at, source_file,
                total_scanned, total_results, avg_score, top_score,
                score_distribution_json, signal_summary_json, sector_leaders_json,
                payload_json, command_archive_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generated_text,
                scan_generated_text,
                source_file or payload.get("_source"),
                payload.get("total_scanned"),
                payload.get("total_results"),
                payload.get("avg_score"),
                payload.get("top_score"),
                self._serialize_payload(payload.get("score_distribution") or []),
                self._serialize_payload(payload.get("signal_summary") or []),
                self._serialize_payload(payload.get("sector_leaders") or []),
                self._serialize_payload(payload) or "{}",
                command_archive_id,
            ),
        )
        return cursor.lastrowid

    def record_report_history(self, market, payload, source_file=None, command_archive_id=None, generated_at=None, scan_generated_at=None):
        def op():
            row_id = self._insert_report_history_row(
                market=market,
                payload=payload,
                source_file=source_file,
                command_archive_id=command_archive_id,
                generated_at=generated_at,
                scan_generated_at=scan_generated_at,
            )
            self.conn.commit()
            return row_id

        return self._run_db_operation("record_report_history", op)

    def _insert_sector_history_rows(self, market, payload, source_file=None, command_archive_id=None, generated_at=None, scan_generated_at=None):
        table_name = self._sector_history_table(market=market)
        if not table_name or not isinstance(payload, dict):
            return 0
        summary_rows = payload.get("sector_summary") or []
        generated_text = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scan_generated_text = scan_generated_at or payload.get("scan_generated_at") or payload.get("generated_at")
        requested_sector = payload.get("sector_filter")
        total_results = payload.get("total_results")
        filtered_count = payload.get("filtered_count")
        inserted = 0
        for rank, row in enumerate(summary_rows, start=1):
            if not isinstance(row, dict):
                continue
            cursor = self.conn.execute(
                f"""
                INSERT INTO {table_name} (
                    generated_at, scan_generated_at, source_file, requested_sector, sector,
                    sector_rank, sector_count, avg_score, best_score, near_high_count,
                    avg_turnover, leader, total_results, filtered_count, payload_json, command_archive_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generated_text,
                    scan_generated_text,
                    source_file or payload.get("source_file") or payload.get("_source"),
                    requested_sector,
                    row.get("sector") or "N/A",
                    rank,
                    row.get("count"),
                    row.get("avg_score"),
                    row.get("best_score"),
                    row.get("near_high_count"),
                    row.get("avg_turnover"),
                    row.get("leader"),
                    total_results,
                    filtered_count,
                    self._serialize_payload(row) or "{}",
                    command_archive_id,
                ),
            )
            inserted += 1 if cursor.rowcount != 0 else 0
        return inserted

    def record_sector_history(self, market, payload, source_file=None, command_archive_id=None, generated_at=None, scan_generated_at=None):
        def op():
            inserted = self._insert_sector_history_rows(
                market=market,
                payload=payload,
                source_file=source_file,
                command_archive_id=command_archive_id,
                generated_at=generated_at,
                scan_generated_at=scan_generated_at,
            )
            self.conn.commit()
            return inserted

        return self._run_db_operation("record_sector_history", op)

    def migrate_legacy_log(self, legacy_path):
        if not legacy_path or not os.path.exists(legacy_path) or self.has_market_state():
            return {"migrated": False, "entries": 0, "tickers": 0}

        def op():
            migrated_entries = 0
            migrated_tickers = set()
            base_datetime = datetime(1970, 1, 1, 0, 0, 0)

            with open(legacy_path) as handle:
                for line_number, line in enumerate(handle, start=1):
                    entry = self.parse_scan_entry(line)
                    if not entry:
                        continue

                    scan_date_text = self._scan_date_to_text(entry["date"])
                    if scan_date_text:
                        scanned_dt = datetime.strptime(scan_date_text, self.log_date_format)
                    else:
                        scanned_dt = base_datetime
                    scanned_at_text = (scanned_dt + timedelta(seconds=line_number)).strftime("%Y-%m-%d %H:%M:%S")

                    self.conn.execute(
                        """
                        INSERT INTO scan_history (
                            market, ticker, score, failed, scan_date, scanned_at, payload_json, run_id, source
                        )
                        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 'legacy')
                        """,
                        (
                            self.market,
                            entry["ticker"],
                            entry["score"],
                            entry["failed"] or "",
                            scan_date_text,
                            scanned_at_text,
                        ),
                    )
                    self.conn.execute(
                        """
                        INSERT INTO latest_scan_state (
                            market, ticker, score, failed, scan_date, scanned_at, payload_json, run_id, source
                        )
                        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 'legacy')
                        ON CONFLICT(market, ticker) DO UPDATE SET
                            score = excluded.score,
                            failed = excluded.failed,
                            scan_date = excluded.scan_date,
                            scanned_at = excluded.scanned_at,
                            payload_json = excluded.payload_json,
                            run_id = excluded.run_id,
                            source = excluded.source
                        """,
                        (
                            self.market,
                            entry["ticker"],
                            entry["score"],
                            entry["failed"] or "",
                            scan_date_text,
                            scanned_at_text,
                        ),
                    )
                    migrated_entries += 1
                    migrated_tickers.add(entry["ticker"])

            self.conn.commit()
            return {
                "migrated": migrated_entries > 0,
                "entries": migrated_entries,
                "tickers": len(migrated_tickers),
            }

        return self._run_db_operation("migrate_legacy_log", op)

    def reset_market(self):
        def op():
            counts = {
                "latest_scan_state": self.conn.execute(
                    "SELECT COUNT(*) FROM latest_scan_state WHERE market = ?",
                    (self.market,),
                ).fetchone()[0],
                "scan_history": self.conn.execute(
                    "SELECT COUNT(*) FROM scan_history WHERE market = ?",
                    (self.market,),
                ).fetchone()[0],
                "scan_runs": self.conn.execute(
                    "SELECT COUNT(*) FROM scan_runs WHERE market = ?",
                    (self.market,),
                ).fetchone()[0],
                "scan_snapshots": self.conn.execute(
                    "SELECT COUNT(*) FROM scan_snapshots WHERE market = ?",
                    (self.market,),
                ).fetchone()[0],
                "scan_artifacts": self.conn.execute(
                    "SELECT COUNT(*) FROM scan_artifacts WHERE market = ?",
                    (self.market,),
                ).fetchone()[0],
                "command_archive": self.conn.execute(
                    f"SELECT COUNT(*) FROM {self._command_archive_table() or 'us_command_archive'}"
                ).fetchone()[0] if self._command_archive_table() else 0,
                "report_history": self.conn.execute(
                    f"SELECT COUNT(*) FROM {self._report_history_table() or 'us_report_history'}"
                ).fetchone()[0] if self._report_history_table() else 0,
                "sector_report_history": self.conn.execute(
                    f"SELECT COUNT(*) FROM {self._sector_history_table() or 'us_sector_report_history'}"
                ).fetchone()[0] if self._sector_history_table() else 0,
            }
            self.conn.execute("DELETE FROM scan_history WHERE market = ?", (self.market,))
            self.conn.execute("DELETE FROM latest_scan_state WHERE market = ?", (self.market,))
            self.conn.execute("DELETE FROM scan_snapshots WHERE market = ?", (self.market,))
            self.conn.execute("DELETE FROM scan_runs WHERE market = ?", (self.market,))
            self.conn.execute("DELETE FROM scan_artifacts WHERE market = ?", (self.market,))
            table_name = self._command_archive_table()
            if table_name:
                self.conn.execute(f"DELETE FROM {table_name}")
            table_name = self._report_history_table()
            if table_name:
                self.conn.execute(f"DELETE FROM {table_name}")
            table_name = self._sector_history_table()
            if table_name:
                self.conn.execute(f"DELETE FROM {table_name}")
            self.conn.commit()
            return counts

        return self._run_db_operation("reset_market", op)

    def has_any_data(self):
        def op():
            for table_name in (
                "latest_scan_state",
                "scan_history",
                "scan_runs",
                "scan_snapshots",
                "scan_artifacts",
                "us_command_archive",
                "india_command_archive",
                "cross_market_command_archive",
                "us_report_history",
                "india_report_history",
                "us_sector_report_history",
                "india_sector_report_history",
            ):
                row = self.conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
                if row is not None:
                    return True
            return False

        return self._run_db_operation("has_any_data", op)


def _delete_sqlite_files(db_path):
    base = Path(db_path)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{base}{suffix}")
        if candidate.exists():
            candidate.unlink()


def _supports_color():
    return sys.stdout.isatty() and os.environ.get("TERM", "") != "dumb"


def _style(text, code):
    if not _supports_color():
        return text
    return f"\033[{code}m{text}\033[0m"


def _truncate(value, width):
    text = "" if value is None else str(value)
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _align(value, width, align="left"):
    text = _truncate(value, width)
    if align == "right":
        return text.rjust(width)
    if align == "center":
        return text.center(width)
    return text.ljust(width)


def _format_number(value, decimals=1):
    if value in (None, "", "N/A"):
        return "—"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if decimals == 0:
        return f"{number:,.0f}"
    return f"{number:,.{decimals}f}"


def _format_metric(value, unit, decimals=1):
    if value in (None, "", "N/A"):
        return "—"
    return f"{_format_number(value, decimals)}{unit}"


def _format_price(value, currency):
    if value in (None, "", "N/A"):
        return "—"
    try:
        return f"{currency}{float(value):,.2f}"
    except (TypeError, ValueError):
        return f"{currency}{value}"


def _format_score(row):
    score = row.get("score")
    max_score = row.get("max_score")
    if score is None:
        return "—"
    if max_score:
        return f"{score}/{max_score}"
    return str(score)


def _format_signals(row, width=38):
    signals = row.get("signals") or []
    if isinstance(signals, list):
        text = " · ".join(str(item) for item in signals if item)
    else:
        text = str(signals)
    return _truncate(text or "—", width)


def _sort_rows(rows, market):
    turnover_key = "avg_turnover_m" if market == "US" else "avg_turnover_cr"
    cap_key = "market_cap_b" if market == "US" else "market_cap_cr"
    return sorted(
        rows,
        key=lambda row: (
            -(row.get("score") or 0),
            -(row.get(turnover_key) or 0),
            -(row.get(cap_key) or 0),
            str(row.get("ticker") or ""),
        ),
    )


def _load_results_file(results_file):
    if not results_file or not os.path.exists(results_file):
        return None
    with open(results_file) as handle:
        payload = json.load(handle)
    payload["_source"] = os.path.basename(results_file)
    payload["stocks"] = payload.get("stocks") or []
    return payload


def _deserialize_payload(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _load_ranked_market_payload(market, results_file=None, db_path=None):
    payload = _load_results_file(results_file)
    if payload is None:
        payload = _load_summary_from_latest_state(db_path, market)
    if payload is None:
        return None

    rows = _sort_rows(payload.get("stocks") or [], market)
    payload["stocks"] = rows
    payload["market"] = market
    payload["total_results"] = len(rows)
    payload["unfiltered_total_results"] = len(rows)
    payload["top_score"] = max((row.get("score") or 0) for row in rows) if rows else 0
    return payload


def _load_summary_from_latest_state(db_path, market):
    if not db_path or not os.path.exists(db_path):
        return None

    store = ScanStateStore(db_path, market=market)
    with store.lock:
        total_scanned = store.conn.execute(
            "SELECT COUNT(*) FROM latest_scan_state WHERE market = ?",
            (market,),
        ).fetchone()[0]
        rows = store.conn.execute(
            """
            SELECT payload_json, scanned_at
            FROM latest_scan_state
            WHERE market = ? AND payload_json IS NOT NULL
            """,
            (market,),
        ).fetchall()
    store.close()

    stocks = []
    latest_generated_at = None
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("_failed"):
            continue
        stocks.append(payload)
        scanned_at = payload.get("scanned_at") or row["scanned_at"]
        if scanned_at and (latest_generated_at is None or scanned_at > latest_generated_at):
            latest_generated_at = scanned_at

    return {
        "generated_at": latest_generated_at,
        "total_scanned": total_scanned,
        "total_results": len(stocks),
        "market": market,
        "stocks": stocks,
        "_source": os.path.basename(db_path),
    }


def _normalize_cap_class(cap_class):
    if not cap_class:
        return None
    value = str(cap_class).strip().lower()
    mapping = {
        "smallcap": "Smallcap",
        "midcap": "Midcap",
        "largecap": "Largecap",
    }
    return mapping.get(value)


def _build_summary_payload(market, results_file=None, db_path=None, limit=30, cap_class=None):
    payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path)
    if payload is None:
        return None

    rows = payload.get("stocks") or []
    payload["cap_class_filter"] = _normalize_cap_class(cap_class)
    payload["unfiltered_total_results"] = len(rows)
    if market == "IN" and payload["cap_class_filter"]:
        rows = [row for row in rows if row.get("cap_category") == payload["cap_class_filter"]]
        payload["total_results"] = len(rows)
    payload["stocks"] = rows[:limit]
    payload["top_score"] = max((row.get("score") or 0) for row in rows) if rows else 0
    return payload


def _metric_value(row, market, metric):
    if metric == "turnover":
        key = "avg_turnover_m" if market == "US" else "avg_turnover_cr"
    elif metric == "cap":
        key = "market_cap_b" if market == "US" else "market_cap_cr"
    else:
        key = metric
    value = row.get(key)
    return value if isinstance(value, (int, float)) else 0


def _parse_iso_date(text):
    if not text or len(text) < 10:
        return None
    candidate = text[:10]
    try:
        datetime.strptime(candidate, "%Y-%m-%d")
        return candidate
    except ValueError:
        return None


def _list_snapshot_files(results_file):
    if not results_file:
        return []
    output_path = Path(results_file)
    if not output_path.parent.exists():
        return []
    snapshots = []
    for candidate in output_path.parent.glob(f"{output_path.stem}_*.json"):
        suffix = candidate.stem[len(output_path.stem) + 1 :]
        try:
            datetime.strptime(suffix, "%Y-%m-%d")
        except ValueError:
            continue
        snapshots.append((suffix, str(candidate)))
    snapshots.sort(key=lambda item: item[0], reverse=True)
    return snapshots


def _find_previous_snapshot(results_file, base_date):
    for snapshot_date, snapshot_path in _list_snapshot_files(results_file):
        if base_date and snapshot_date >= base_date:
            continue
        payload = _load_results_file(snapshot_path)
        if payload is not None:
            payload["_snapshot_date"] = snapshot_date
            payload["_snapshot_path"] = snapshot_path
            return payload
    return None


def _load_archive_run_state_map(market, archive_db, scan_generated_at=None, base_date=None):
    if not archive_db or not os.path.exists(archive_db):
        return {"rows": {}}

    store = ScanStateStore(archive_db, market=market)
    try:
        with store.lock:
            if scan_generated_at:
                run_row = store.conn.execute(
                    """
                    SELECT id, run_date, completed_at
                    FROM scan_runs
                    WHERE market = ?
                      AND status = 'completed'
                      AND completed_at <= ?
                    ORDER BY completed_at DESC, id DESC
                    LIMIT 1
                    """,
                    (market, scan_generated_at),
                ).fetchone()
            elif base_date:
                run_row = store.conn.execute(
                    """
                    SELECT id, run_date, completed_at
                    FROM scan_runs
                    WHERE market = ?
                      AND status = 'completed'
                      AND run_date <= ?
                    ORDER BY run_date DESC, id DESC
                    LIMIT 1
                    """,
                    (market, base_date),
                ).fetchone()
            else:
                run_row = store.conn.execute(
                    """
                    SELECT id, run_date, completed_at
                    FROM scan_runs
                    WHERE market = ?
                      AND status = 'completed'
                    ORDER BY completed_at DESC, id DESC
                    LIMIT 1
                    """,
                    (market,),
                ).fetchone()
            if run_row is None:
                return {"rows": {}}
            history_rows = store.conn.execute(
                """
                SELECT ticker, score, failed, payload_json
                FROM scan_history
                WHERE market = ?
                  AND run_id = ?
                ORDER BY id ASC
                """,
                (market, run_row["id"]),
            ).fetchall()
    finally:
        store.close()

    payload_map = {}
    for row in history_rows:
        payload_map[row["ticker"]] = {
            "score": row["score"],
            "failed": row["failed"],
            "payload": _deserialize_payload(row["payload_json"]),
        }
    return {
        "run_id": run_row["id"],
        "run_date": run_row["run_date"],
        "completed_at": run_row["completed_at"],
        "rows": payload_map,
    }


def _load_previous_archive_run_payload(market, archive_db, base_date):
    if not archive_db or not os.path.exists(archive_db):
        return None

    store = ScanStateStore(archive_db, market=market)
    try:
        with store.lock:
            params = [market]
            date_filter = ""
            if base_date:
                date_filter = "AND run_date < ?"
                params.append(base_date)
            run_row = store.conn.execute(
                f"""
                SELECT id, run_date, completed_at, total_results
                FROM scan_runs
                WHERE market = ?
                  AND status = 'completed'
                  {date_filter}
                ORDER BY run_date DESC, id DESC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            if run_row is None:
                return None
            history_rows = store.conn.execute(
                """
                SELECT ticker, payload_json
                FROM scan_history
                WHERE market = ?
                  AND run_id = ?
                  AND failed = ''
                ORDER BY id ASC
                """,
                (market, run_row["id"]),
            ).fetchall()
    finally:
        store.close()

    payload_map = {}
    for row in history_rows:
        payload = _deserialize_payload(row["payload_json"])
        if not isinstance(payload, dict):
            continue
        ticker = payload.get("ticker") or row["ticker"]
        if not ticker:
            continue
        payload.setdefault("ticker", ticker)
        payload_map[ticker] = payload

    if not payload_map:
        return None

    return {
        "generated_at": run_row["completed_at"] or run_row["run_date"],
        "stocks": list(payload_map.values()),
        "_source": f"{os.path.basename(archive_db)} (run {run_row['id']})",
        "_snapshot_date": run_row["run_date"],
        "_archive_run_id": run_row["id"],
        "_archive_total_results": run_row["total_results"],
    }


def _signal_reason_bucket(signal):
    text = str(signal or "").strip()
    if not text:
        return None
    for prefix in (
        "Near 52W High",
        "Above EMA21",
        "Above EMA50",
        "Above EMA200",
        "Volume Spike",
        "Upper Circuit",
        "Delivery",
        "EPS Growth",
        "Rev Growth",
    ):
        if text.startswith(prefix):
            return prefix
    return text


def _unique_preserve_order(items):
    output = []
    seen = set()
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _format_reason_items(items, limit=3):
    values = _unique_preserve_order(items)
    if not values:
        return ""
    trimmed = values[:limit]
    text = ", ".join(trimmed)
    if len(values) > limit:
        text += f" +{len(values) - limit} more"
    return text


def _build_score_change_reason(current_row=None, previous_row=None):
    current_buckets = [_signal_reason_bucket(item) for item in (current_row or {}).get("signals") or []]
    previous_buckets = [_signal_reason_bucket(item) for item in (previous_row or {}).get("signals") or []]
    current_buckets = _unique_preserve_order(current_buckets)
    previous_buckets = _unique_preserve_order(previous_buckets)
    lost = [item for item in previous_buckets if item not in current_buckets]
    gained = [item for item in current_buckets if item not in previous_buckets]
    parts = []
    lost_text = _format_reason_items(lost)
    gained_text = _format_reason_items(gained)
    if lost_text:
        parts.append(f"Lost: {lost_text}")
    if gained_text:
        parts.append(f"Gained: {gained_text}")
    if parts:
        return "; ".join(parts)
    previous_score = (previous_row or {}).get("score")
    current_score = (current_row or {}).get("score")
    if previous_score is not None and current_score is not None and previous_score != current_score:
        return f"Score changed from {previous_score} to {current_score}"
    return "Signal mix changed"


def _build_drop_reason(current_state):
    if not current_state:
        return "Dropped from current results; current scan detail unavailable"
    payload = current_state.get("payload")
    failed = current_state.get("failed")
    if isinstance(payload, dict):
        status = payload.get("status")
        if status == "failed_primary":
            codes = payload.get("failed_codes") or [item for item in str(failed or "").split("|") if item]
            message = payload.get("message")
            if codes and message:
                return f"Failed primary ({', '.join(codes)}): {message}"
            if message:
                return message
            if codes:
                return f"Failed primary ({', '.join(codes)})"
            return "Failed primary conditions"
        if status == "no_data":
            return "No data / insufficient history in current scan"
        if status == "error":
            return f"Current scan error: {payload.get('message') or 'Unknown'}"
    if failed:
        return f"Current scan failed: {failed}"
    return "Dropped from current results"


def _build_ranked_table_data(market, rows):
    if market == "US":
        headers = ["#", "Ticker", "Name", "Score", "Price", "Turn(M)", "MCap(B)", "PE", "Signals"]
        table_rows = []
        for idx, row in enumerate(rows, start=1):
            table_rows.append(
                [
                    _align(idx, 2, "right"),
                    _truncate(row.get("ticker") or "—", 8),
                    _truncate(row.get("name") or "—", 26),
                    _align(_format_score(row), 5, "right"),
                    _align(_format_price(row.get("price"), row.get("currency", "$")), 9, "right"),
                    _align(_format_metric(row.get("avg_turnover_m"), "M", 1), 8, "right"),
                    _align(_format_metric(row.get("market_cap_b"), "B", 3), 9, "right"),
                    _align(_format_number(row.get("pe"), 1), 6, "right"),
                    _format_signals(row, width=38),
                ]
            )
    else:
        headers = ["#", "Ticker", "Name", "Score", "Price", "Turn(Cr)", "Cap(Cr)", "Class", "Signals"]
        table_rows = []
        for idx, row in enumerate(rows, start=1):
            table_rows.append(
                [
                    _align(idx, 2, "right"),
                    _truncate(row.get("ticker") or "—", 10),
                    _truncate(row.get("name") or "—", 24),
                    _align(_format_score(row), 5, "right"),
                    _align(_format_price(row.get("price"), row.get("currency", "₹")), 10, "right"),
                    _align(_format_metric(row.get("avg_turnover_cr"), "Cr", 1), 10, "right"),
                    _align(_format_metric(row.get("market_cap_cr"), "Cr", 0), 12, "right"),
                    _truncate(row.get("cap_category") or row.get("exchange") or "—", 8),
                    _format_signals(row, width=34),
                ]
            )
    aligns = ["right", "left", "left", "right", "right", "right", "right", "left", "left"]
    return headers, table_rows, aligns


def _build_score_distribution(rows, market):
    exact_counts = Counter(int(row.get("score") or 0) for row in rows)
    max_score = 7 if market == "US" else 9
    distribution = []
    total = len(rows) or 1
    for score in range(max_score, 0, -1):
        count = exact_counts.get(score, 0)
        if count == 0:
            continue
        distribution.append(
            {
                "score": score,
                "count": count,
                "share": round((count / total) * 100, 1),
            }
        )
    return distribution


def _build_signal_summary(rows):
    labels = [
        "Near 52W High",
        "Above EMA21",
        "Above EMA50",
        "Above EMA200",
        "Volume Spike",
        "EPS Growth",
        "Rev Growth",
        "Upper Circuit",
        "Delivery",
    ]
    output = []
    for label in labels:
        count = 0
        for row in rows:
            signals = row.get("signals") or []
            if any(str(signal).startswith(label) for signal in signals):
                count += 1
        if count:
            output.append({"label": label, "count": count})
    return output


def _build_sector_leaders(rows):
    sectors = defaultdict(list)
    for row in rows:
        sector = row.get("sector") or "N/A"
        if sector == "N/A":
            continue
        sectors[sector].append(row)
    leaders = []
    for sector, sector_rows in sectors.items():
        sorted_rows = sorted(
            sector_rows,
            key=lambda row: (-(row.get("score") or 0), -_metric_value(row, row.get("market"), "turnover"), str(row.get("ticker") or "")),
        )
        avg_score = sum(row.get("score") or 0 for row in sector_rows) / len(sector_rows)
        leaders.append(
            {
                "sector": sector,
                "count": len(sector_rows),
                "avg_score": round(avg_score, 2),
                "best_score": max(row.get("score") or 0 for row in sector_rows),
                "leader": sorted_rows[0].get("ticker") or "—",
            }
        )
    return sorted(leaders, key=lambda item: (-item["count"], -item["avg_score"], item["sector"]))


def _build_report_payload(market, results_file=None, db_path=None, limit=30):
    payload = _build_summary_payload(market=market, results_file=results_file, db_path=db_path, limit=limit)
    if payload is None:
        return None

    full_payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path)
    rows = (full_payload or {}).get("stocks") or []
    if not rows:
        payload["score_distribution"] = []
        payload["signal_summary"] = []
        payload["sector_leaders"] = []
        payload["avg_score"] = 0
        return payload

    payload["stocks"] = rows[:limit]
    payload["avg_score"] = round(sum(row.get("score") or 0 for row in rows) / len(rows), 2)
    payload["score_distribution"] = _build_score_distribution(rows, market)
    payload["signal_summary"] = _build_signal_summary(rows)
    payload["sector_leaders"] = _build_sector_leaders(rows)[:10]
    return payload


def _build_diff_payload(market, results_file, archive_db=None, limit=15):
    current_payload = _load_results_file(results_file)
    if current_payload is None:
        return None

    current_rows = _sort_rows(current_payload.get("stocks") or [], market)
    base_date = _parse_iso_date(current_payload.get("generated_at"))
    previous_payload = _find_previous_snapshot(results_file, base_date)
    if previous_payload is None:
        previous_payload = _load_previous_archive_run_payload(market, archive_db, base_date)
    current_archive_state = _load_archive_run_state_map(
        market,
        archive_db,
        scan_generated_at=current_payload.get("generated_at"),
        base_date=base_date,
    )
    current_state_map = current_archive_state.get("rows") or {}
    current_map = {row.get("ticker"): row for row in current_rows if row.get("ticker")}

    diff_payload = {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": current_payload.get("generated_at"),
        "source_file": current_payload.get("_source"),
        "current_total": len(current_rows),
        "previous_total": 0,
        "baseline_found": previous_payload is not None,
        "reference_date": None,
        "reference_file": None,
        "new_entries": [],
        "upgrades": [],
        "downgrades": [],
        "dropped": [],
        "unchanged_count": 0,
    }

    if previous_payload is None:
        return diff_payload

    previous_rows = _sort_rows(previous_payload.get("stocks") or [], market)
    previous_map = {row.get("ticker"): row for row in previous_rows if row.get("ticker")}
    diff_payload["previous_total"] = len(previous_rows)
    diff_payload["reference_date"] = previous_payload.get("_snapshot_date") or _parse_iso_date(previous_payload.get("generated_at"))
    diff_payload["reference_file"] = previous_payload.get("_source")

    def make_entry(current_row=None, previous_row=None):
        row = current_row or previous_row or {}
        ticker = row.get("ticker") or "—"
        current_score = current_row.get("score") if current_row else None
        previous_score = previous_row.get("score") if previous_row else None
        delta = None
        if current_score is not None and previous_score is not None:
            delta = current_score - previous_score
        entry = {
            "ticker": ticker,
            "name": (current_row or previous_row or {}).get("name") or ticker,
            "current_score": current_score,
            "previous_score": previous_score,
            "delta": delta,
            "price": row.get("price"),
            "currency": row.get("currency", "$" if market == "US" else "₹"),
            "signals": (current_row or previous_row or {}).get("signals") or [],
            "turnover": _metric_value(row, market, "turnover"),
            "reason": "",
        }
        if previous_row is None and current_row is not None:
            entry["reason"] = "Newly qualified in current scan"
        elif current_row is None and previous_row is not None:
            entry["reason"] = _build_drop_reason(current_state_map.get(ticker))
        elif current_row is not None and previous_row is not None and current_score != previous_score:
            entry["reason"] = _build_score_change_reason(current_row=current_row, previous_row=previous_row)
        return entry

    for ticker, current_row in current_map.items():
        previous_row = previous_map.get(ticker)
        if previous_row is None:
            diff_payload["new_entries"].append(make_entry(current_row=current_row))
            continue
        current_score = current_row.get("score") or 0
        previous_score = previous_row.get("score") or 0
        if current_score > previous_score:
            diff_payload["upgrades"].append(make_entry(current_row=current_row, previous_row=previous_row))
        elif current_score < previous_score:
            diff_payload["downgrades"].append(make_entry(current_row=current_row, previous_row=previous_row))
        else:
            diff_payload["unchanged_count"] += 1

    for ticker, previous_row in previous_map.items():
        if ticker not in current_map:
            diff_payload["dropped"].append(make_entry(previous_row=previous_row))

    diff_payload["new_entries"] = sorted(
        diff_payload["new_entries"],
        key=lambda item: (-(item.get("current_score") or 0), -item.get("turnover", 0), item["ticker"]),
    )[:limit]
    diff_payload["upgrades"] = sorted(
        diff_payload["upgrades"],
        key=lambda item: (-(item.get("delta") or 0), -(item.get("current_score") or 0), -item.get("turnover", 0), item["ticker"]),
    )[:limit]
    diff_payload["downgrades"] = sorted(
        diff_payload["downgrades"],
        key=lambda item: ((item.get("delta") or 0), -(item.get("current_score") or 0), -item.get("turnover", 0), item["ticker"]),
    )[:limit]
    diff_payload["dropped"] = sorted(
        diff_payload["dropped"],
        key=lambda item: (-(item.get("previous_score") or 0), -item.get("turnover", 0), item["ticker"]),
    )[:limit]
    return diff_payload


def _render_table(headers, rows, aligns):
    widths = []
    for index, header in enumerate(headers):
        cell_width = len(header)
        for row in rows:
            cell_width = max(cell_width, len(str(row[index])))
        widths.append(cell_width)

    top = "┌" + "┬".join("─" * (width + 2) for width in widths) + "┐"
    mid = "├" + "┼".join("─" * (width + 2) for width in widths) + "┤"
    bot = "└" + "┴".join("─" * (width + 2) for width in widths) + "┘"

    lines = [top]
    header_cells = [_align(header, widths[i], "center") for i, header in enumerate(headers)]
    lines.append("│ " + " │ ".join(header_cells) + " │")
    lines.append(mid)
    for row in rows:
        cells = [_align(row[i], widths[i], aligns[i]) for i in range(len(headers))]
        lines.append("│ " + " │ ".join(cells) + " │")
    lines.append(bot)
    return "\n".join(lines)


def _paint_table(table_text, border_code="96", header_code="97", row_palette=None):
    row_palette = row_palette or ["38;5;214", "97", "38;5;118", "92", "96"]
    lines = table_text.splitlines()
    painted = []
    data_index = 0
    for idx, line in enumerate(lines):
        if idx in (0, 2, len(lines) - 1):
            painted.append(_style(line, border_code))
        elif idx == 1:
            painted.append(_style(line, f"1;{header_code}"))
        else:
            painted.append(_style(line, row_palette[data_index % len(row_palette)]))
            data_index += 1
    return "\n".join(painted)


def _print_summary_table(payload, market, limit):
    cap_filter = payload.get("cap_class_filter")
    if market == "IN" and cap_filter:
        label = f"INDIA {cap_filter.upper()} TABLE"
    else:
        label = "US MARKET SUMMARY" if market == "US" else "INDIA MARKET SUMMARY"
    accent = "96" if market == "US" else "38;5;214"
    title = _style(label, f"1;{accent}")
    generated_at = payload.get("generated_at") or "Unknown"
    total_scanned = payload.get("total_scanned") or 0
    total_results = payload.get("total_results") or 0
    top_score = payload.get("top_score") or 0
    source = payload.get("_source") or "unknown"
    terminal_width = max(96, min(shutil.get_terminal_size((120, 24)).columns, 140))
    rail = _style("═" * terminal_width, accent)

    print("")
    print(rail)
    print(title)
    print(_style(f"Latest scan: {generated_at}  |  Source: {source}", "2"))
    if market == "IN" and cap_filter:
        unfiltered_total = payload.get("unfiltered_total_results") or total_results
        print(_style(f"Cap class: {cap_filter}  |  Matching results: {total_results} of {unfiltered_total}  |  Total scanned: {total_scanned}  |  Best score: {top_score}", "2"))
    else:
        print(_style(f"Showing top {min(limit, total_results)} of {total_results} results  |  Total scanned: {total_scanned}  |  Best score: {top_score}", "2"))
    print(rail)

    headers, rows, aligns = _build_ranked_table_data(market, payload.get("stocks") or [])

    if not rows:
        print(_style("No qualified results found in the latest scan.", "33"))
        print(rail)
        print("")
        return

    table = _render_table(headers, rows, aligns)
    if market == "IN" and cap_filter:
        table = _paint_table(table, border_code="96", header_code="97", row_palette=["38;5;214", "97", "38;5;118", "92", "96"])
    print(table)
    print(rail)
    print("")


def _print_report(payload, market, limit):
    label = "US MARKET REPORT" if market == "US" else "INDIA MARKET REPORT"
    accent = "96" if market == "US" else "38;5;214"
    title = _style(label, f"1;{accent}")
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(title)
    print(_style(f"Latest scan: {payload.get('generated_at') or 'Unknown'}  |  Source: {payload.get('_source') or 'unknown'}", "2"))
    print(_style(f"Top table rows: {min(limit, payload.get('total_results') or 0)}  |  Total scanned: {payload.get('total_scanned') or 0}", "2"))
    print(rail)

    total_results = payload.get("total_results") or 0
    avg_score = payload.get("avg_score") or 0
    top_score = payload.get("top_score") or 0
    print(f"Results: {total_results}  |  Avg score: {avg_score}  |  Best score: {top_score}")

    score_rows = [
        [
            str(item["score"]),
            str(item["count"]),
            f"{item['share']:.1f}%",
        ]
        for item in payload.get("score_distribution") or []
    ]
    if score_rows:
        print("")
        print(_style("Score Distribution", f"1;{accent}"))
        print(_render_table(["Score", "Count", "Share"], score_rows, ["right", "right", "right"]))

    signal_rows = [
        [
            _truncate(item["label"], 18),
            str(item["count"]),
        ]
        for item in payload.get("signal_summary") or []
    ]
    if signal_rows:
        print("")
        print(_style("Signal Counts", f"1;{accent}"))
        print(_render_table(["Signal", "Count"], signal_rows, ["left", "right"]))

    sector_rows = [
        [
            _truncate(item["sector"], 22),
            str(item["count"]),
            _format_number(item["avg_score"], 2),
            str(item["best_score"]),
            item["leader"],
        ]
        for item in payload.get("sector_leaders") or []
    ]
    if sector_rows:
        print("")
        print(_style("Sector Leaders", f"1;{accent}"))
        print(_render_table(["Sector", "Count", "Avg", "Best", "Leader"], sector_rows, ["left", "right", "right", "right", "left"]))

    print("")
    print(_style("Top Ranked Names", f"1;{accent}"))
    headers, rows, aligns = _build_ranked_table_data(market, payload.get("stocks") or [])
    if rows:
        print(_render_table(headers, rows, aligns))
    else:
        print(_style("No qualified results found in the latest scan.", "33"))
    print(rail)
    print("")


def _diff_table_rows(rows, market, dropped=False):
    turnover_unit = "M" if market == "US" else "Cr"
    output = []
    for row in rows:
        current_score = row.get("current_score")
        previous_score = row.get("previous_score")
        delta = row.get("delta")
        if delta is None:
            delta_text = "NEW" if previous_score is None else "DROP"
        elif delta > 0:
            delta_text = f"+{delta}"
        else:
            delta_text = str(delta)
        output.append(
            [
                _truncate(row.get("ticker") or "—", 10),
                _truncate(row.get("name") or "—", 24),
                "—" if current_score is None else str(current_score),
                "—" if previous_score is None else str(previous_score),
                delta_text,
                _format_price(row.get("price"), row.get("currency", "$" if market == "US" else "₹")),
                _format_metric(row.get("turnover"), turnover_unit, 1),
                _truncate(row.get("reason") or "—", 40),
                _truncate(" · ".join(row.get("signals") or []) or ("Dropped from current results" if dropped else "—"), 30),
            ]
        )
    return output


def _print_diff(payload, market):
    label = "US MARKET DIFF" if market == "US" else "INDIA MARKET DIFF"
    accent = "96" if market == "US" else "38;5;214"
    title = _style(label, f"1;{accent}")
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(title)
    print(_style(f"Latest scan: {payload.get('scan_generated_at') or 'Unknown'}  |  Source: {payload.get('source_file') or 'unknown'}", "2"))
    if payload.get("baseline_found"):
        print(_style(f"Baseline snapshot: {payload.get('reference_date') or 'Unknown'}  |  File: {payload.get('reference_file') or 'unknown'}", "2"))
    else:
        print(_style("Baseline snapshot: not available yet", "2"))
    print(rail)
    print(
        f"Current results: {payload.get('current_total') or 0}  |  "
        f"Previous results: {payload.get('previous_total') or 0}  |  "
        f"New: {len(payload.get('new_entries') or [])}  |  "
        f"Up: {len(payload.get('upgrades') or [])}  |  "
        f"Down: {len(payload.get('downgrades') or [])}  |  "
        f"Dropped: {len(payload.get('dropped') or [])}  |  "
        f"Unchanged: {payload.get('unchanged_count') or 0}"
    )

    if not payload.get("baseline_found"):
        print("")
        print(_style("No older snapshot exists yet, so diff details will populate after the next full scan day.", "33"))
        print(rail)
        print("")
        return

    sections = [
        ("New Entries", payload.get("new_entries") or [], False),
        ("Upgrades", payload.get("upgrades") or [], False),
        ("Downgrades", payload.get("downgrades") or [], False),
        ("Dropped", payload.get("dropped") or [], True),
    ]
    headers = ["Ticker", "Name", "Now", "Prev", "Δ", "Price", "Turn", "Reason", "Notes"]
    aligns = ["left", "left", "right", "right", "right", "right", "right", "left", "left"]
    for title_text, section_rows, dropped in sections:
        print("")
        print(_style(title_text, f"1;{accent}"))
        if section_rows:
            print(_render_table(headers, _diff_table_rows(section_rows, market, dropped=dropped), aligns))
        else:
            print(_style("No rows in this section.", "2"))

    print(rail)
    print("")


def _parse_generated_at(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _format_age_from_datetime(value):
    if value is None:
        return "—"
    delta = datetime.now() - value
    total_minutes = max(0, int(delta.total_seconds() // 60))
    hours, minutes = divmod(total_minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _signal_present(row, label):
    return any(str(signal).startswith(label) for signal in row.get("signals") or [])


def _market_turnover_label(market):
    return "M" if market == "US" else "Cr"


def _load_latest_payload_map(db_path, market):
    if not db_path or not os.path.exists(db_path):
        return {}
    store = ScanStateStore(db_path, market=market)
    with store.lock:
        rows = store.conn.execute(
            """
            SELECT ticker, payload_json
            FROM latest_scan_state
            WHERE market = ?
            """,
            (market,),
        ).fetchall()
    store.close()

    payload_map = {}
    for row in rows:
        payload = _deserialize_payload(row["payload_json"])
        if isinstance(payload, dict):
            payload_map[row["ticker"]] = payload
    return payload_map


def _record_archive_artifact(archive_db, market, artifact_type, payload, scan_generated_at=None, source_file=None, reference_file=None, reference_date=None):
    if not archive_db:
        return None
    store = ScanStateStore(archive_db, market=market)
    artifact_id = store.record_artifact(
        artifact_type=artifact_type,
        payload=payload,
        scan_generated_at=scan_generated_at,
        source_file=source_file,
        reference_file=reference_file,
        reference_date=reference_date,
    )
    command_archive_id = store.record_command_output(
        command_name=artifact_type,
        payload=payload,
        market=market,
        scan_generated_at=scan_generated_at,
        source_file=source_file,
        reference_file=reference_file,
        reference_date=reference_date,
        legacy_artifact_id=artifact_id,
    )
    if artifact_type == "report" and market in ("US", "IN"):
        store.record_report_history(
            market=market,
            payload=payload,
            source_file=source_file,
            command_archive_id=command_archive_id,
            scan_generated_at=scan_generated_at,
        )
    elif artifact_type == "sector-report" and market in ("US", "IN"):
        store.record_sector_history(
            market=market,
            payload=payload,
            source_file=source_file,
            command_archive_id=command_archive_id,
            scan_generated_at=scan_generated_at,
        )
    store.close()
    return command_archive_id or artifact_id


def _artifact_note(artifact_type, payload):
    payload = payload if isinstance(payload, dict) else {}
    if artifact_type == "archive-query":
        command_filter = payload.get("command_filter") or "all"
        return f"matches {payload.get('matched_count', 0)} | cmd {command_filter}"
    if artifact_type == "summary":
        cap_filter = payload.get("cap_class_filter")
        if cap_filter:
            return f"{cap_filter} | results {payload.get('total_results', 0)} | best {payload.get('top_score', 0)}"
        return f"results {payload.get('total_results', 0)} | best {payload.get('top_score', 0)}"
    if artifact_type == "history":
        summary = payload.get("summary") or {}
        return (
            f"events {summary.get('total_events', 0)} | "
            f"tickers {summary.get('unique_tickers', 0)}"
        )
    if artifact_type == "diff":
        if not payload.get("baseline_found"):
            return f"curr {payload.get('current_total', 0)} | no baseline"
        return (
            f"curr {payload.get('current_total', 0)} | "
            f"new {len(payload.get('new_entries') or [])} | "
            f"up {len(payload.get('upgrades') or [])} | "
            f"down {len(payload.get('downgrades') or [])} | "
            f"drop {len(payload.get('dropped') or [])}"
        )
    if artifact_type == "report":
        return (
            f"results {payload.get('total_results', 0)} | "
            f"avg {payload.get('avg_score', 0)} | best {payload.get('top_score', 0)}"
        )
    if artifact_type == "artifact-history":
        return f"artifacts {payload.get('artifact_count', 0)}"
    if artifact_type == "ticker-history":
        return (
            f"{payload.get('ticker', '—')} | "
            f"scans {payload.get('total_scans', 0)} | "
            f"best {payload.get('best_score', 0)}"
        )
    if artifact_type == "leaderboard":
        return (
            f"current {payload.get('current_total', 0)} | "
            f"consistent {len(payload.get('consistent_leaders') or [])}"
        )
    if artifact_type == "sector-history":
        sector_filter = payload.get("sector_filter")
        if sector_filter:
            return f"{sector_filter} | rows {payload.get('matched_count', 0)}"
        return f"rows {payload.get('matched_count', 0)} | latest {payload.get('latest_scan_generated_at') or '—'}"
    if artifact_type == "sector-report":
        sector_filter = payload.get("sector_filter")
        if sector_filter:
            return f"{sector_filter} | rows {payload.get('filtered_count', 0)}"
        return f"sectors {len(payload.get('sector_summary') or [])}"
    if artifact_type == "new-highs":
        return f"count {payload.get('new_high_count', 0)} | best {payload.get('top_score', 0)}"
    if artifact_type == "compare-markets":
        us = (payload.get("markets") or {}).get("US", {})
        india = (payload.get("markets") or {}).get("IN", {})
        return f"US {us.get('total_results', 0)} | IN {india.get('total_results', 0)}"
    if artifact_type == "doctor":
        return f"{payload.get('overall_status', 'UNKNOWN')} | warns {payload.get('warning_count', 0)} | fails {payload.get('fail_count', 0)}"
    if artifact_type == "export":
        return (
            f"{payload.get('dataset', 'market')} | rows {payload.get('rows_exported', 0)} | "
            f"files {len(payload.get('files') or [])}"
        )
    return "stored"


def _command_archive_specs(scope_market, include_shared=True):
    market = (scope_market or "ALL").upper()
    if market == "US":
        specs = [("US", "us_command_archive")]
        if include_shared:
            specs.append(("ALL", "cross_market_command_archive"))
        return specs
    if market == "IN":
        specs = [("IN", "india_command_archive")]
        if include_shared:
            specs.append(("ALL", "cross_market_command_archive"))
        return specs
    return [
        ("US", "us_command_archive"),
        ("IN", "india_command_archive"),
        ("ALL", "cross_market_command_archive"),
    ]


def _load_command_archive_rows(db_path, market, command_name=None, include_shared=True):
    if not db_path or not os.path.exists(db_path):
        return []
    scope_market = market or "ALL"
    store = ScanStateStore(db_path, market=scope_market)
    rows = []
    with store.lock:
        for table_market, table_name in _command_archive_specs(scope_market, include_shared=include_shared):
            if command_name:
                query = f"""
                    SELECT id, ? AS market, command_name AS artifact_type, generated_at, scan_generated_at,
                           source_file, reference_file, reference_date, payload_json
                    FROM {table_name}
                    WHERE command_name = ?
                    ORDER BY generated_at DESC, id DESC
                """
                query_params = (table_market, command_name)
            else:
                query = f"""
                    SELECT id, ? AS market, command_name AS artifact_type, generated_at, scan_generated_at,
                           source_file, reference_file, reference_date, payload_json
                    FROM {table_name}
                    ORDER BY generated_at DESC, id DESC
                """
                query_params = (table_market,)
            rows.extend(store.conn.execute(query, query_params).fetchall())
    store.close()

    parsed_rows = []
    for row in rows:
        item = dict(row)
        item["payload"] = _deserialize_payload(item.pop("payload_json"))
        item["note"] = _artifact_note(item["artifact_type"], item["payload"])
        parsed_rows.append(item)
    parsed_rows.sort(key=lambda row: (row["generated_at"], row["id"]), reverse=True)
    return parsed_rows


def _build_artifact_history_payload(db_path, market, artifact_type=None, limit=20, include_shared=True):
    if not db_path or not os.path.exists(db_path):
        return None
    scope_market = market or "ALL"
    parsed_rows = _load_command_archive_rows(
        db_path=db_path,
        market=scope_market,
        command_name=artifact_type,
        include_shared=include_shared,
    )

    grouped = defaultdict(list)
    for row in parsed_rows:
        grouped[row["artifact_type"]].append(row)

    summary_rows = []
    for type_name, type_rows in sorted(grouped.items(), key=lambda item: item[0]):
        summary_rows.append(
            {
                "artifact_type": type_name,
                "count": len(type_rows),
                "latest_generated_at": type_rows[0]["generated_at"],
                "markets": ",".join(sorted({row["market"] for row in type_rows})),
                "note": type_rows[0]["note"],
            }
        )

    return {
        "market_scope": scope_market,
        "artifact_filter": artifact_type,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "artifact_count": len(parsed_rows),
        "summary_rows": summary_rows,
        "recent_artifacts": parsed_rows[:limit],
    }


def _archive_query_entity(command_name, payload):
    payload = payload if isinstance(payload, dict) else {}
    if command_name == "ticker-history":
        return payload.get("ticker") or "—"
    if command_name == "summary" and payload.get("cap_class_filter"):
        return payload.get("cap_class_filter")
    if command_name == "sector-report" and payload.get("sector_filter"):
        return payload.get("sector_filter")
    if command_name == "compare-markets":
        return "US vs IN"
    if command_name == "history":
        return payload.get("market") or "—"
    if payload.get("market"):
        return payload.get("market")
    return "—"


def _build_archive_query_payload(db_path, market, command_name=None, search=None, limit=25, include_shared=True):
    if not db_path or not os.path.exists(db_path):
        return None
    rows = _load_command_archive_rows(
        db_path=db_path,
        market=market,
        command_name=command_name,
        include_shared=include_shared,
    )
    needle = (search or "").strip().lower()
    filtered_rows = []
    for row in rows:
        entity = _archive_query_entity(row.get("artifact_type"), row.get("payload"))
        haystack_parts = [
            row.get("artifact_type") or "",
            row.get("note") or "",
            entity or "",
            os.path.basename(row.get("source_file") or ""),
            os.path.basename(row.get("reference_file") or ""),
            json.dumps(row.get("payload") or {}, ensure_ascii=True, default=str),
        ]
        if needle and not any(needle in part.lower() for part in haystack_parts if isinstance(part, str)):
            continue
        row["entity"] = entity
        filtered_rows.append(row)

    return {
        "market_scope": market or "ALL",
        "command_filter": command_name,
        "search_filter": search,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "matched_count": len(filtered_rows),
        "rows": filtered_rows[:limit],
    }


def _display_scan_event_time(scanned_at, scan_date, source):
    if source == "legacy":
        if scan_date:
            return scan_date
        if scanned_at and not str(scanned_at).startswith("1970-01-01"):
            return scanned_at
        return "legacy import"
    return scanned_at or scan_date or "—"


def _build_ticker_history_payload(market, db_path, ticker, limit=20):
    if not db_path or not os.path.exists(db_path):
        return None
    normalized_ticker = (ticker or "").strip().upper()
    if not normalized_ticker:
        return None

    store = ScanStateStore(db_path, market=market)
    with store.lock:
        rows = store.conn.execute(
            """
            SELECT ticker, score, failed, scan_date, scanned_at, payload_json, source
            FROM scan_history
            WHERE market = ? AND UPPER(ticker) = ?
            ORDER BY scanned_at DESC, id DESC
            """,
            (market, normalized_ticker),
        ).fetchall()
    store.close()

    if not rows:
        return None

    parsed_rows = []
    latest_payload = None
    for row in rows:
        payload = _deserialize_payload(row["payload_json"])
        item = dict(row)
        item["payload"] = payload if isinstance(payload, dict) else None
        parsed_rows.append(item)
        if latest_payload is None and isinstance(payload, dict) and not payload.get("_failed"):
            latest_payload = payload

    scores = [int(row["score"] or 0) for row in parsed_rows]
    latest_row = parsed_rows[0]
    latest_payload = latest_payload or {}
    latest_name = latest_payload.get("name") or latest_row["ticker"]
    high_score_hits = sum(1 for score in scores if score >= 5)
    near_high_hits = sum(1 for row in parsed_rows if row["payload"] and _signal_present(row["payload"], "Near 52W High"))

    profile = {
        "name": latest_name,
        "sector": latest_payload.get("sector") or "—",
        "industry": latest_payload.get("industry") or "—",
        "price": latest_payload.get("price"),
        "turnover": _metric_value(latest_payload, market, "turnover") if latest_payload else None,
        "market_cap": _metric_value(latest_payload, market, "cap") if latest_payload else None,
        "signals": latest_payload.get("signals") or [],
    }

    first_seen = None
    for row in reversed(parsed_rows):
        display_when = _display_scan_event_time(row["scanned_at"], row["scan_date"], row["source"])
        if display_when and display_when != "legacy import":
            first_seen = display_when
            break
    if first_seen is None:
        oldest = parsed_rows[-1]
        first_seen = _display_scan_event_time(oldest["scanned_at"], oldest["scan_date"], oldest["source"])

    recent_events = []
    for row in parsed_rows[:limit]:
        payload = row["payload"] or {}
        recent_events.append(
            {
                "scan_date": row["scan_date"] or "—",
                "scanned_at": row["scanned_at"],
                "display_when": _display_scan_event_time(row["scanned_at"], row["scan_date"], row["source"]),
                "score": row["score"],
                "failed": row["failed"] or "—",
                "source": row["source"],
                "signals": " · ".join(payload.get("signals") or []) or "—",
            }
        )

    return {
        "market": market,
        "ticker": normalized_ticker,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_score": latest_row["score"],
        "best_score": max(scores) if scores else 0,
        "avg_score": round(sum(scores) / len(scores), 2) if scores else 0,
        "total_scans": len(parsed_rows),
        "high_score_hits": high_score_hits,
        "near_high_hits": near_high_hits,
        "first_seen": first_seen,
        "last_scanned_at": _display_scan_event_time(latest_row["scanned_at"], latest_row["scan_date"], latest_row["source"]),
        "profile": profile,
        "recent_events": recent_events,
    }


def _build_leaderboard_payload(market, results_file=None, db_path=None, history_db=None, limit=15):
    current_payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path or history_db)
    if not history_db or not os.path.exists(history_db):
        return None

    store = ScanStateStore(history_db, market=market)
    with store.lock:
        rows = store.conn.execute(
            """
            SELECT ticker, score, scan_date, scanned_at, payload_json
            FROM scan_history
            WHERE market = ?
            ORDER BY scanned_at DESC, id DESC
            """,
            (market,),
        ).fetchall()
    store.close()
    if not rows and current_payload is None:
        return None

    latest_payload_map = _load_latest_payload_map(history_db, market)
    name_lookup = {ticker: payload.get("name") or ticker for ticker, payload in latest_payload_map.items()}
    stats = defaultdict(lambda: {"scores": [], "last_scanned_at": None, "best_score": 0})
    high_hits = Counter()
    near_high_hits = Counter()

    for row in rows:
        ticker = row["ticker"]
        score = int(row["score"] or 0)
        stats[ticker]["scores"].append(score)
        stats[ticker]["best_score"] = max(stats[ticker]["best_score"], score)
        if stats[ticker]["last_scanned_at"] is None:
            stats[ticker]["last_scanned_at"] = row["scanned_at"]
        if score >= 5:
            high_hits[ticker] += 1
        payload = _deserialize_payload(row["payload_json"])
        if isinstance(payload, dict) and _signal_present(payload, "Near 52W High"):
            near_high_hits[ticker] += 1

    consistent = []
    for ticker, item in stats.items():
        scans = len(item["scores"])
        if scans < 2:
            continue
        consistent.append(
            {
                "ticker": ticker,
                "name": name_lookup.get(ticker, ticker),
                "avg_score": round(sum(item["scores"]) / scans, 2),
                "best_score": item["best_score"],
                "scans": scans,
                "last_scanned_at": item["last_scanned_at"],
            }
        )
    consistent.sort(key=lambda row: (-row["avg_score"], -row["best_score"], -row["scans"], row["ticker"]))

    high_score_rows = []
    for ticker, hits in high_hits.items():
        high_score_rows.append(
            {
                "ticker": ticker,
                "name": name_lookup.get(ticker, ticker),
                "hits": hits,
                "best_score": stats[ticker]["best_score"],
                "scans": len(stats[ticker]["scores"]),
                "last_scanned_at": stats[ticker]["last_scanned_at"],
            }
        )
    high_score_rows.sort(key=lambda row: (-row["hits"], -row["best_score"], -row["scans"], row["ticker"]))

    near_high_rows = []
    for ticker, hits in near_high_hits.items():
        near_high_rows.append(
            {
                "ticker": ticker,
                "name": name_lookup.get(ticker, ticker),
                "hits": hits,
                "best_score": stats[ticker]["best_score"],
                "scans": len(stats[ticker]["scores"]),
                "last_scanned_at": stats[ticker]["last_scanned_at"],
            }
        )
    near_high_rows.sort(key=lambda row: (-row["hits"], -row["best_score"], -row["scans"], row["ticker"]))

    return {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": (current_payload or {}).get("generated_at"),
        "current_total": len((current_payload or {}).get("stocks") or []),
        "history_events": len(rows),
        "unique_tickers": len(stats),
        "current_leaders": ((current_payload or {}).get("stocks") or [])[:limit],
        "consistent_leaders": consistent[:limit],
        "high_score_hits": high_score_rows[:limit],
        "near_high_hits": near_high_rows[:limit],
    }


def _build_sector_summary_rows(rows, market):
    sectors = defaultdict(list)
    for row in rows:
        sector = row.get("sector") or "N/A"
        if sector == "N/A":
            continue
        sectors[sector].append(row)

    summary_rows = []
    for sector, sector_rows in sectors.items():
        sorted_rows = _sort_rows(sector_rows, market)
        avg_score = round(sum((row.get("score") or 0) for row in sector_rows) / len(sector_rows), 2)
        avg_turnover = round(sum(_metric_value(row, market, "turnover") for row in sector_rows) / len(sector_rows), 2)
        summary_rows.append(
            {
                "sector": sector,
                "count": len(sector_rows),
                "avg_score": avg_score,
                "best_score": max(row.get("score") or 0 for row in sector_rows),
                "near_high_count": sum(1 for row in sector_rows if _signal_present(row, "Near 52W High")),
                "avg_turnover": avg_turnover,
                "leader": sorted_rows[0].get("ticker") or "—",
            }
        )
    summary_rows.sort(key=lambda row: (-row["count"], -row["avg_score"], row["sector"]))
    return summary_rows


def _build_sector_report_payload(market, results_file=None, db_path=None, sector=None, limit=30):
    payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path)
    if payload is None:
        return None

    all_rows = payload.get("stocks") or []
    sector_summary = _build_sector_summary_rows(all_rows, market)
    sector_filter = (sector or "").strip()
    matched_sector = None
    filtered_rows = all_rows

    if sector_filter:
        normalized = sector_filter.lower()
        exact = [row["sector"] for row in sector_summary if row["sector"].lower() == normalized]
        if exact:
            matched_sector = exact[0]
        else:
            partial = [row["sector"] for row in sector_summary if normalized in row["sector"].lower()]
            if partial:
                matched_sector = partial[0]
        filtered_rows = [row for row in all_rows if matched_sector and row.get("sector") == matched_sector] if matched_sector else []
    filtered_rows = _sort_rows(filtered_rows, market)

    return {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": payload.get("generated_at"),
        "source_file": payload.get("_source"),
        "sector_filter": matched_sector,
        "filtered_count": len(filtered_rows),
        "total_results": len(all_rows),
        "sector_summary": sector_summary,
        "stocks": filtered_rows[:limit] if matched_sector else [],
    }


def _match_sector_name(options, sector_query):
    query = (sector_query or "").strip().lower()
    if not query:
        return None
    exact = [option for option in options if option.lower() == query]
    if exact:
        return exact[0]
    partial = [option for option in options if query in option.lower()]
    if partial:
        return partial[0]
    return None


def _delta_or_none(current, previous, decimals=2):
    if current is None or previous is None:
        return None
    try:
        value = float(current) - float(previous)
    except (TypeError, ValueError):
        return None
    if decimals == 0:
        return int(round(value))
    return round(value, decimals)


def _build_sector_history_payload(market, archive_db, sector=None, limit=20):
    if not archive_db or not os.path.exists(archive_db):
        return None

    store = ScanStateStore(archive_db, market=market)
    table_name = store._sector_history_table(market)
    if not table_name:
        store.close()
        return None

    with store.lock:
        sector_rows = store.conn.execute(f"SELECT DISTINCT sector FROM {table_name} ORDER BY sector ASC").fetchall()
        sectors = [row["sector"] for row in sector_rows if row["sector"]]
        latest_scan_row = store.conn.execute(
            f"""
            SELECT scan_generated_at
            FROM {table_name}
            WHERE scan_generated_at IS NOT NULL
            ORDER BY scan_generated_at DESC, generated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        latest_scan = latest_scan_row["scan_generated_at"] if latest_scan_row else None
        previous_scan = None
        if latest_scan:
            previous_row = store.conn.execute(
                f"""
                SELECT scan_generated_at
                FROM {table_name}
                WHERE scan_generated_at < ?
                ORDER BY scan_generated_at DESC, generated_at DESC, id DESC
                LIMIT 1
                """,
                (latest_scan,),
            ).fetchone()
            previous_scan = previous_row["scan_generated_at"] if previous_row else None

        matched_sector = _match_sector_name(sectors, sector)
        if matched_sector:
            rows = store.conn.execute(
                f"""
                SELECT generated_at, scan_generated_at, source_file, requested_sector, sector,
                       sector_rank, sector_count, avg_score, best_score, near_high_count,
                       avg_turnover, leader, total_results, filtered_count
                FROM {table_name}
                WHERE id IN (
                    SELECT MAX(id)
                    FROM {table_name}
                    WHERE sector = ?
                    GROUP BY scan_generated_at
                )
                ORDER BY scan_generated_at DESC, generated_at DESC, id DESC
                LIMIT ?
                """,
                (matched_sector, limit),
            ).fetchall()
        elif latest_scan:
            rows = store.conn.execute(
                f"""
                SELECT generated_at, scan_generated_at, source_file, requested_sector, sector,
                       sector_rank, sector_count, avg_score, best_score, near_high_count,
                       avg_turnover, leader, total_results, filtered_count
                FROM {table_name}
                WHERE id IN (
                    SELECT MAX(id)
                    FROM {table_name}
                    WHERE scan_generated_at = ?
                    GROUP BY sector
                )
                ORDER BY sector_rank ASC, sector ASC
                LIMIT ?
                """,
                (latest_scan, limit),
            ).fetchall()
        else:
            rows = []

        previous_rows = {}
        if previous_scan and not matched_sector:
            prev_result = store.conn.execute(
                f"""
                SELECT sector, sector_count, avg_score, near_high_count
                FROM {table_name}
                WHERE id IN (
                    SELECT MAX(id)
                    FROM {table_name}
                    WHERE scan_generated_at = ?
                    GROUP BY sector
                )
                """,
                (previous_scan,),
            ).fetchall()
            previous_rows = {row["sector"]: dict(row) for row in prev_result}
    store.close()

    if not rows:
        return {
            "market": market,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "sector" if matched_sector else "snapshot",
            "sector_filter": matched_sector,
            "latest_scan_generated_at": latest_scan,
            "previous_scan_generated_at": previous_scan,
            "rows": [],
            "matched_count": 0,
        }

    output_rows = []
    if matched_sector:
        normalized = [dict(row) for row in rows]
        for index, row in enumerate(normalized):
            older = normalized[index + 1] if index + 1 < len(normalized) else None
            output_rows.append(
                {
                    **row,
                    "delta_count": _delta_or_none(row.get("sector_count"), (older or {}).get("sector_count"), 0),
                    "delta_avg_score": _delta_or_none(row.get("avg_score"), (older or {}).get("avg_score"), 2),
                    "delta_near_high": _delta_or_none(row.get("near_high_count"), (older or {}).get("near_high_count"), 0),
                }
            )
        mode = "sector"
    else:
        for row in rows:
            item = dict(row)
            previous = previous_rows.get(item["sector"], {})
            output_rows.append(
                {
                    **item,
                    "delta_count": _delta_or_none(item.get("sector_count"), previous.get("sector_count"), 0),
                    "delta_avg_score": _delta_or_none(item.get("avg_score"), previous.get("avg_score"), 2),
                    "delta_near_high": _delta_or_none(item.get("near_high_count"), previous.get("near_high_count"), 0),
                }
            )
        mode = "snapshot"

    return {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "sector_filter": matched_sector,
        "latest_scan_generated_at": latest_scan,
        "previous_scan_generated_at": previous_scan,
        "rows": output_rows,
        "matched_count": len(output_rows),
    }


def _build_new_highs_payload(market, results_file=None, db_path=None, limit=30):
    payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path)
    if payload is None:
        return None
    rows = [row for row in payload.get("stocks") or [] if _signal_present(row, "Near 52W High")]
    rows = _sort_rows(rows, market)
    return {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": payload.get("generated_at"),
        "source_file": payload.get("_source"),
        "total_results": len(payload.get("stocks") or []),
        "new_high_count": len(rows),
        "top_score": max((row.get("score") or 0) for row in rows) if rows else 0,
        "sector_summary": _build_sector_summary_rows(rows, market),
        "stocks": rows[:limit],
    }


def _build_market_compare_summary(payload, market):
    rows = payload.get("stocks") or []
    sector_summary = _build_sector_summary_rows(rows, market)
    return {
        "market": market,
        "generated_at": payload.get("generated_at"),
        "total_scanned": payload.get("total_scanned") or 0,
        "total_results": len(rows),
        "avg_score": round(sum((row.get("score") or 0) for row in rows) / len(rows), 2) if rows else 0,
        "top_score": max((row.get("score") or 0) for row in rows) if rows else 0,
        "score_ge_5": sum(1 for row in rows if (row.get("score") or 0) >= 5),
        "near_high_count": sum(1 for row in rows if _signal_present(row, "Near 52W High")),
        "volume_spike_count": sum(1 for row in rows if _signal_present(row, "Volume Spike")),
        "top_sector": sector_summary[0]["sector"] if sector_summary else "—",
        "leader": rows[0].get("ticker") if rows else "—",
        "sector_summary": sector_summary[:8],
        "top_rows": rows[:10],
    }


def _build_compare_markets_payload(us_results_file=None, india_results_file=None, db_path=None):
    us_payload = _load_ranked_market_payload("US", results_file=us_results_file, db_path=db_path)
    india_payload = _load_ranked_market_payload("IN", results_file=india_results_file, db_path=db_path)
    if us_payload is None or india_payload is None:
        return None
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "markets": {
            "US": _build_market_compare_summary(us_payload, "US"),
            "IN": _build_market_compare_summary(india_payload, "IN"),
        },
    }


def _build_doctor_payload(market, results_file=None, state_db=None, archive_db=None, venv_dir=None, scanner_file=None, requirements_file=None):
    checks = []

    def add_check(name, status, details):
        checks.append({"name": name, "status": status, "details": details})

    if scanner_file:
        add_check("scanner_file", "PASS" if os.path.exists(scanner_file) else "FAIL", scanner_file)
    if venv_dir:
        add_check("venv", "PASS" if os.path.isdir(venv_dir) else "WARN", venv_dir)
    if requirements_file:
        add_check("requirements", "PASS" if os.path.exists(requirements_file) else "WARN", requirements_file)

    payload = _load_ranked_market_payload(market, results_file=results_file, db_path=state_db or archive_db)
    if payload is None:
        add_check("results_json", "FAIL", f"missing: {results_file or 'N/A'}")
    else:
        generated_at = payload.get("generated_at")
        generated_dt = _parse_generated_at(generated_at)
        add_check(
            "results_json",
            "PASS",
            f"{payload.get('_source', 'results')} | updated {generated_at or 'Unknown'} | age {_format_age_from_datetime(generated_dt)}",
        )
        add_check("results_count", "PASS" if payload.get("total_results", 0) > 0 else "WARN", f"{payload.get('total_results', 0)} qualified rows")

    snapshots = _list_snapshot_files(results_file)
    add_check("snapshots", "PASS" if snapshots else "WARN", f"{len(snapshots)} snapshot files")
    previous_exists = False
    if payload is not None:
        previous_exists = _find_previous_snapshot(results_file, _parse_iso_date(payload.get("generated_at"))) is not None
    add_check("diff_baseline", "PASS" if previous_exists else "WARN", "older snapshot available" if previous_exists else "no older snapshot yet")

    if state_db:
        if os.path.exists(state_db):
            store = ScanStateStore(state_db, market=market)
            with store.lock:
                latest_count = store.conn.execute("SELECT COUNT(*) FROM latest_scan_state WHERE market = ?", (market,)).fetchone()[0]
                run_count = store.conn.execute("SELECT COUNT(*) FROM scan_runs WHERE market = ?", (market,)).fetchone()[0]
            store.close()
            add_check("state_db", "PASS", f"{os.path.basename(state_db)} | latest {latest_count} | runs {run_count}")
        else:
            add_check("state_db", "FAIL", f"missing: {state_db}")

    artifact_count = 0
    if archive_db:
        if os.path.exists(archive_db):
            store = ScanStateStore(archive_db, market=market)
            with store.lock:
                history_count = store.conn.execute("SELECT COUNT(*) FROM scan_history WHERE market = ?", (market,)).fetchone()[0]
                artifact_count = store.conn.execute("SELECT COUNT(*) FROM scan_artifacts WHERE market IN (?, 'ALL')", (market,)).fetchone()[0]
                last_artifact = store.conn.execute(
                    "SELECT generated_at FROM scan_artifacts WHERE market IN (?, 'ALL') ORDER BY generated_at DESC, id DESC LIMIT 1",
                    (market,),
                ).fetchone()
            store.close()
            details = f"{os.path.basename(archive_db)} | events {history_count} | artifacts {artifact_count}"
            if last_artifact:
                details += f" | last artifact {last_artifact['generated_at']}"
            add_check("archive_db", "PASS", details)
        else:
            add_check("archive_db", "FAIL", f"missing: {archive_db}")

    fail_count = sum(1 for check in checks if check["status"] == "FAIL")
    warning_count = sum(1 for check in checks if check["status"] == "WARN")
    overall_status = "FAIL" if fail_count else ("WARN" if warning_count else "PASS")
    return {
        "market": market,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall_status": overall_status,
        "fail_count": fail_count,
        "warning_count": warning_count,
        "checks": checks,
        "artifact_count": artifact_count,
    }


def _render_markdown_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def _normalized_export_format(value):
    text = (value or "both").strip().lower().lstrip(".")
    if text in ("markdown", "md"):
        return "md"
    if text in ("csv",):
        return "csv"
    return "both"


def _export_file_bundle(export_root, base_stem, export_format, csv_headers=None, csv_rows=None, md_lines=None):
    export_root.mkdir(parents=True, exist_ok=True)
    files = []
    if export_format in ("csv", "both") and csv_headers is not None:
        csv_path = export_root / f"{base_stem}.csv"
        with csv_path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(csv_headers)
            writer.writerows(csv_rows or [])
        files.append({"kind": "csv", "path": str(csv_path)})
    if export_format in ("md", "both") and md_lines is not None:
        md_path = export_root / f"{base_stem}.md"
        md_path.write_text("\n".join(md_lines))
        files.append({"kind": "markdown", "path": str(md_path)})
    return files


def _slugify_text(value):
    text = (value or "").strip().lower()
    safe = []
    for char in text:
        if char.isalnum():
            safe.append(char)
        else:
            safe.append("_")
    slug = "".join(safe)
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_") or "view"


def _write_csv_table(export_root, stem, headers, rows):
    export_root.mkdir(parents=True, exist_ok=True)
    csv_path = export_root / f"{stem}.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows or [])
    return {"kind": "csv", "path": str(csv_path)}


def _write_md_document(export_root, stem, lines):
    export_root.mkdir(parents=True, exist_ok=True)
    md_path = export_root / f"{stem}.md"
    md_path.write_text("\n".join(lines))
    return {"kind": "markdown", "path": str(md_path)}


def _ranked_csv_headers_rows(market, rows):
    if market == "US":
        headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverM", "MarketCapB", "PE", "Sector", "Industry", "Signals"]
        csv_rows = [
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("score") or 0,
                row.get("price") or "",
                row.get("avg_turnover_m") or "",
                row.get("market_cap_b") or "",
                row.get("pe") or "",
                row.get("sector") or "",
                row.get("industry") or "",
                " | ".join(row.get("signals") or []),
            ]
            for row in rows or []
        ]
    else:
        headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverCr", "MarketCapCr", "CapCategory", "PE", "Sector", "Industry", "Signals"]
        csv_rows = [
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("score") or 0,
                row.get("price") or "",
                row.get("avg_turnover_cr") or "",
                row.get("market_cap_cr") or "",
                row.get("cap_category") or "",
                row.get("pe") or "",
                row.get("sector") or "",
                row.get("industry") or "",
                " | ".join(row.get("signals") or []),
            ]
            for row in rows or []
        ]
    return headers, csv_rows


def _market_export_dir(workflow_root, market):
    return workflow_root / market.lower()


def _workflow_root_dir(out_dir, origin_market, workflow_name):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    root = Path(out_dir or Path.cwd() / "exports" / "workflows")
    bundle_dir = root / f"{origin_market.lower()}_{_slugify_text(workflow_name)}_{timestamp}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return bundle_dir


def _export_summary_view(market, results_file=None, db_path=None, export_root=None, limit=30, cap_class=None):
    payload = _build_summary_payload(market=market, results_file=results_file, db_path=db_path, limit=limit, cap_class=cap_class)
    if payload is None:
        return None
    market_root = _market_export_dir(export_root, market)
    cap_suffix = f"_{_slugify_text(payload.get('cap_class_filter'))}" if payload.get("cap_class_filter") else ""
    stem = f"{market.lower()}_summary{cap_suffix}"
    csv_headers, csv_rows = _ranked_csv_headers_rows(market, payload.get("stocks") or [])
    md_headers, md_rows, _ = _build_ranked_table_data(market, payload.get("stocks") or [])
    md_lines = [
        f"# {market} Summary Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Latest Scan: {payload.get('generated_at') or 'Unknown'}",
        f"- Source: {payload.get('_source') or 'Unknown'}",
        f"- Cap Filter: {payload.get('cap_class_filter') or 'None'}",
        f"- Total Results: {payload.get('total_results') or 0}",
        "",
        "## Ranked Table",
        _render_markdown_table(md_headers, md_rows) if md_rows else "_No rows_",
    ]
    files = [
        _write_csv_table(market_root, stem, csv_headers, csv_rows),
        _write_md_document(market_root, stem, md_lines),
    ]
    return {
        "dataset": "summary",
        "market": market,
        "scan_generated_at": payload.get("generated_at"),
        "rows_exported": len(csv_rows),
        "files": files,
        "cap_class_filter": payload.get("cap_class_filter"),
    }


def _export_report_view(market, results_file=None, db_path=None, export_root=None, limit=30):
    payload = _build_report_payload(market=market, results_file=results_file, db_path=db_path, limit=limit)
    if payload is None:
        return None
    market_root = _market_export_dir(export_root, market)
    base = f"{market.lower()}_report"

    score_headers = ["Score", "Count", "SharePct"]
    score_rows = [[item["score"], item["count"], item["share"]] for item in payload.get("score_distribution") or []]
    signal_headers = ["Signal", "Count"]
    signal_rows = [[item["label"], item["count"]] for item in payload.get("signal_summary") or []]
    sector_headers = ["Sector", "Count", "AvgScore", "BestScore", "Leader"]
    sector_rows = [[item["sector"], item["count"], item["avg_score"], item["best_score"], item["leader"]] for item in payload.get("sector_leaders") or []]
    ranked_headers, ranked_rows = _ranked_csv_headers_rows(market, payload.get("stocks") or [])
    md_rank_headers, md_rank_rows, _ = _build_ranked_table_data(market, payload.get("stocks") or [])

    files = [
        _write_csv_table(market_root, f"{base}_score_distribution", score_headers, score_rows),
        _write_csv_table(market_root, f"{base}_signal_summary", signal_headers, signal_rows),
        _write_csv_table(market_root, f"{base}_sector_leaders", sector_headers, sector_rows),
        _write_csv_table(market_root, f"{base}_top_ranked", ranked_headers, ranked_rows),
    ]
    md_lines = [
        f"# {market} Report Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Latest Scan: {payload.get('generated_at') or 'Unknown'}",
        f"- Source: {payload.get('_source') or 'Unknown'}",
        f"- Total Results: {payload.get('total_results') or 0}",
        f"- Total Scanned: {payload.get('total_scanned') or 0}",
        f"- Avg Score: {payload.get('avg_score') or 0}",
        f"- Top Score: {payload.get('top_score') or 0}",
        "",
        "## Score Distribution",
        _render_markdown_table(score_headers, score_rows) if score_rows else "_No rows_",
        "",
        "## Signal Summary",
        _render_markdown_table(signal_headers, signal_rows) if signal_rows else "_No rows_",
        "",
        "## Sector Leaders",
        _render_markdown_table(sector_headers, sector_rows) if sector_rows else "_No rows_",
        "",
        "## Top Ranked Names",
        _render_markdown_table(md_rank_headers, md_rank_rows) if md_rank_rows else "_No rows_",
    ]
    files.append(_write_md_document(market_root, base, md_lines))
    return {
        "dataset": "report",
        "market": market,
        "scan_generated_at": payload.get("generated_at"),
        "rows_exported": len(ranked_rows),
        "files": files,
    }


def _export_diff_view(market, results_file, export_root=None, archive_db=None, limit=15):
    payload = _build_diff_payload(market=market, results_file=results_file, archive_db=archive_db, limit=limit)
    if payload is None:
        return None
    market_root = _market_export_dir(export_root, market)
    stem = f"{market.lower()}_diff"
    section_specs = [
        ("New Entries", payload.get("new_entries") or [], False),
        ("Upgrades", payload.get("upgrades") or [], False),
        ("Downgrades", payload.get("downgrades") or [], False),
        ("Dropped", payload.get("dropped") or [], True),
    ]
    csv_headers = ["Section", "Ticker", "Name", "CurrentScore", "PreviousScore", "Delta", "Price", "Turnover", "Reason", "Notes"]
    csv_rows = []
    md_lines = [
        f"# {market} Diff Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Latest Scan: {payload.get('scan_generated_at') or 'Unknown'}",
        f"- Baseline Found: {'Yes' if payload.get('baseline_found') else 'No'}",
        f"- Reference Date: {payload.get('reference_date') or 'Unknown'}",
        f"- Current Results: {payload.get('current_total') or 0}",
        f"- Previous Results: {payload.get('previous_total') or 0}",
        "",
    ]
    for section_name, rows, dropped in section_specs:
        detail_rows = []
        for row in rows:
            notes = " · ".join(row.get("signals") or []) or ("Dropped from current results" if dropped else "—")
            detail_row = [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("current_score") if row.get("current_score") is not None else "",
                row.get("previous_score") if row.get("previous_score") is not None else "",
                row.get("delta") if row.get("delta") is not None else "",
                row.get("price") or "",
                row.get("turnover") or "",
                row.get("reason") or "",
                notes,
            ]
            detail_rows.append(detail_row)
            csv_rows.append([section_name] + detail_row)
        md_lines.extend([
            f"## {section_name}",
            _render_markdown_table(["Ticker", "Name", "Now", "Prev", "Delta", "Price", "Turnover", "Reason", "Notes"], detail_rows) if detail_rows else "_No rows_",
            "",
        ])
    files = [
        _write_csv_table(market_root, stem, csv_headers, csv_rows),
        _write_md_document(market_root, stem, md_lines),
    ]
    return {
        "dataset": "diff",
        "market": market,
        "scan_generated_at": payload.get("scan_generated_at"),
        "rows_exported": len(csv_rows),
        "files": files,
    }


def _export_new_highs_view(market, results_file=None, db_path=None, export_root=None, limit=30):
    payload = _build_new_highs_payload(market=market, results_file=results_file, db_path=db_path, limit=limit)
    if payload is None:
        return None
    market_root = _market_export_dir(export_root, market)
    base = f"{market.lower()}_new_highs"
    sector_headers = ["Sector", "Count", "AvgScore", "BestScore", "Leader"]
    sector_rows = [[item["sector"], item["count"], item["avg_score"], item["best_score"], item["leader"]] for item in payload.get("sector_summary") or []]
    ranked_headers, ranked_rows = _ranked_csv_headers_rows(market, payload.get("stocks") or [])
    md_rank_headers, md_rank_rows, _ = _build_ranked_table_data(market, payload.get("stocks") or [])
    files = [
        _write_csv_table(market_root, f"{base}_sector_breakdown", sector_headers, sector_rows),
        _write_csv_table(market_root, f"{base}_ranked", ranked_headers, ranked_rows),
    ]
    md_lines = [
        f"# {market} Near 52W High Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Latest Scan: {payload.get('scan_generated_at') or 'Unknown'}",
        f"- Near High Count: {payload.get('new_high_count') or 0}",
        "",
        "## Sector Breakdown",
        _render_markdown_table(sector_headers, sector_rows) if sector_rows else "_No rows_",
        "",
        "## Ranked Names",
        _render_markdown_table(md_rank_headers, md_rank_rows) if md_rank_rows else "_No rows_",
    ]
    files.append(_write_md_document(market_root, base, md_lines))
    return {
        "dataset": "new-highs",
        "market": market,
        "scan_generated_at": payload.get("scan_generated_at"),
        "rows_exported": len(ranked_rows),
        "files": files,
    }


def _export_doctor_view(market, results_file=None, state_db=None, archive_db=None, venv_dir=None, scanner_file=None, requirements_file=None, export_root=None):
    payload = _build_doctor_payload(
        market=market,
        results_file=results_file,
        state_db=state_db,
        archive_db=archive_db,
        venv_dir=venv_dir,
        scanner_file=scanner_file,
        requirements_file=requirements_file,
    )
    if payload is None:
        return None
    market_root = _market_export_dir(export_root, market)
    stem = f"{market.lower()}_doctor"
    csv_headers = ["Check", "Status", "Details"]
    csv_rows = [[item["name"], item["status"], item["details"]] for item in payload.get("checks") or []]
    md_lines = [
        f"# {market} Doctor Export",
        "",
        f"- Generated: {payload.get('generated_at') or 'Unknown'}",
        f"- Overall Status: {payload.get('overall_status') or 'UNKNOWN'}",
        f"- Warnings: {payload.get('warning_count') or 0}",
        f"- Failures: {payload.get('fail_count') or 0}",
        "",
        "## Checks",
        _render_markdown_table(csv_headers, csv_rows) if csv_rows else "_No rows_",
    ]
    files = [
        _write_csv_table(market_root, stem, csv_headers, csv_rows),
        _write_md_document(market_root, stem, md_lines),
    ]
    return {
        "dataset": "doctor",
        "market": market,
        "scan_generated_at": payload.get("generated_at"),
        "rows_exported": len(csv_rows),
        "files": files,
    }


def _export_compare_markets_view(us_results_file=None, india_results_file=None, db_path=None, export_root=None):
    payload = _build_compare_markets_payload(us_results_file=us_results_file, india_results_file=india_results_file, db_path=db_path)
    if payload is None:
        return None
    cross_root = Path(export_root or Path.cwd()) / "cross_market"
    cross_root.mkdir(parents=True, exist_ok=True)
    stem = "cross_market_compare"
    overview_headers = ["Market", "Scan", "Scanned", "Results", "AvgScore", "TopScore", "ScoreGE5", "NearHigh", "VolumeSpike", "TopSector", "Leader"]
    overview_rows = []
    md_lines = [
        "# Cross-Market Compare Export",
        "",
        f"- Generated: {payload.get('generated_at') or 'Unknown'}",
        "",
    ]
    for market in ("US", "IN"):
        item = (payload.get("markets") or {}).get(market) or {}
        overview_rows.append([
            market,
            item.get("generated_at") or "",
            item.get("total_scanned") or 0,
            item.get("total_results") or 0,
            item.get("avg_score") or 0,
            item.get("top_score") or 0,
            item.get("score_ge_5") or 0,
            item.get("near_high_count") or 0,
            item.get("volume_spike_count") or 0,
            item.get("top_sector") or "",
            item.get("leader") or "",
        ])
    files = [_write_csv_table(cross_root, f"{stem}_overview", overview_headers, overview_rows)]
    md_lines.extend(["## Overview", _render_markdown_table(overview_headers, overview_rows), ""])
    for market in ("US", "IN"):
        item = (payload.get("markets") or {}).get(market) or {}
        sector_headers = ["Sector", "Count", "AvgScore", "BestScore", "Leader"]
        sector_rows = [[row["sector"], row["count"], row["avg_score"], row["best_score"], row["leader"]] for row in item.get("sector_summary") or []]
        ranked_headers, ranked_rows = _ranked_csv_headers_rows(market, item.get("top_rows") or [])
        md_rank_headers, md_rank_rows, _ = _build_ranked_table_data(market, item.get("top_rows") or [])
        files.append(_write_csv_table(cross_root, f"{stem}_{market.lower()}_sector_leaders", sector_headers, sector_rows))
        files.append(_write_csv_table(cross_root, f"{stem}_{market.lower()}_top_ranked", ranked_headers, ranked_rows))
        md_lines.extend([
            f"## {market} Sector Leaders",
            _render_markdown_table(sector_headers, sector_rows) if sector_rows else "_No rows_",
            "",
            f"## {market} Top Ranked",
            _render_markdown_table(md_rank_headers, md_rank_rows) if md_rank_rows else "_No rows_",
            "",
        ])
    files.append(_write_md_document(cross_root, stem, md_lines))
    return {
        "dataset": "compare-markets",
        "market": "ALL",
        "scan_generated_at": payload.get("generated_at"),
        "rows_exported": len(overview_rows),
        "files": files,
    }


def _build_workflow_export_payload(workflow_name, origin_market, us_results_file=None, india_results_file=None, db_path=None, archive_db=None, out_dir=None, limit=30, us_venv_dir=None, india_venv_dir=None, us_scanner_file=None, india_scanner_file=None, us_requirements_file=None):
    workflow_slug = _slugify_text(workflow_name)
    bundle_root = _workflow_root_dir(out_dir, origin_market, workflow_slug)
    files = []
    views = []

    def add_view(payload):
        if not payload:
            return
        files.extend(payload.get("files") or [])
        views.append({
            "dataset": payload.get("dataset"),
            "market": payload.get("market"),
            "rows_exported": payload.get("rows_exported") or 0,
        })

    add_view(_export_report_view("IN", results_file=india_results_file, db_path=db_path, export_root=bundle_root, limit=limit))
    add_view(_export_diff_view("IN", results_file=india_results_file, export_root=bundle_root, archive_db=archive_db, limit=15))
    add_view(_export_report_view("US", results_file=us_results_file, db_path=db_path, export_root=bundle_root, limit=limit))
    add_view(_export_diff_view("US", results_file=us_results_file, export_root=bundle_root, archive_db=archive_db, limit=15))
    add_view(_export_compare_markets_view(us_results_file=us_results_file, india_results_file=india_results_file, db_path=db_path, export_root=bundle_root))

    if workflow_slug == "daily_full":
        add_view(_export_doctor_view("IN", results_file=india_results_file, state_db=db_path, archive_db=archive_db, venv_dir=india_venv_dir, scanner_file=india_scanner_file, export_root=bundle_root))
        add_view(_export_doctor_view("US", results_file=us_results_file, state_db=db_path, archive_db=archive_db, venv_dir=us_venv_dir, scanner_file=us_scanner_file, requirements_file=us_requirements_file, export_root=bundle_root))
        add_view(_build_sector_report_export_payload("IN", results_file=india_results_file, db_path=db_path, archive_db=None, out_dir=bundle_root, export_format="both", limit=max(30, limit)))
        add_view(_build_sector_history_export_payload("IN", archive_db=archive_db or db_path, out_dir=bundle_root, export_format="both", limit=max(20, limit)))
        add_view(_export_new_highs_view("IN", results_file=india_results_file, db_path=db_path, export_root=bundle_root, limit=limit))
        add_view(_export_summary_view("IN", results_file=india_results_file, db_path=db_path, export_root=bundle_root, limit=limit, cap_class="smallcap"))
        add_view(_export_summary_view("IN", results_file=india_results_file, db_path=db_path, export_root=bundle_root, limit=limit, cap_class="midcap"))
        add_view(_export_summary_view("IN", results_file=india_results_file, db_path=db_path, export_root=bundle_root, limit=limit, cap_class="largecap"))
        add_view(_build_sector_report_export_payload("US", results_file=us_results_file, db_path=db_path, archive_db=None, out_dir=bundle_root, export_format="both", limit=max(30, limit)))
        add_view(_build_sector_history_export_payload("US", archive_db=archive_db or db_path, out_dir=bundle_root, export_format="both", limit=max(20, limit)))
        add_view(_export_new_highs_view("US", results_file=us_results_file, db_path=db_path, export_root=bundle_root, limit=limit))

    payload = {
        "market": origin_market,
        "dataset": f"workflow-{workflow_slug}",
        "workflow": workflow_slug,
        "format": "both",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows_exported": len(files),
        "bundle_dir": str(bundle_root),
        "files": files,
        "views": views,
    }
    if archive_db:
        payload["artifact_id"] = _record_archive_artifact(
            archive_db,
            origin_market,
            "export",
            payload,
            scan_generated_at=payload.get("scan_generated_at"),
            source_file=str(bundle_root),
        )
    return payload


def _build_market_export_payload(market, results_file=None, db_path=None, archive_db=None, out_dir=None, export_format="both"):
    payload = _load_ranked_market_payload(market, results_file=results_file, db_path=db_path)
    if payload is None:
        return None
    report_payload = _build_report_payload(market, results_file=results_file, db_path=db_path, limit=30)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_root = Path(out_dir or Path.cwd() / "exports") / market.lower()
    export_root.mkdir(parents=True, exist_ok=True)

    if market == "US":
        headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverM", "MarketCapB", "PE", "ForwardPE", "ProfitMargin", "Sector", "Industry", "Signals"]
        rows = [
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("score") or 0,
                row.get("price") or "",
                row.get("avg_turnover_m") or "",
                row.get("market_cap_b") or "",
                row.get("pe") or "",
                row.get("forward_pe") or "",
                row.get("profit_margin") or "",
                row.get("sector") or "",
                row.get("industry") or "",
                " | ".join(row.get("signals") or []),
            ]
            for row in payload.get("stocks") or []
        ]
    else:
        headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverCr", "MarketCapCr", "CapCategory", "PE", "ForwardPE", "ProfitMargin", "Sector", "Industry", "Signals"]
        rows = [
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("score") or 0,
                row.get("price") or "",
                row.get("avg_turnover_cr") or "",
                row.get("market_cap_cr") or "",
                row.get("cap_category") or "",
                row.get("pe") or "",
                row.get("forward_pe") or "",
                row.get("profit_margin") or "",
                row.get("sector") or "",
                row.get("industry") or "",
                " | ".join(row.get("signals") or []),
            ]
            for row in payload.get("stocks") or []
        ]

    md_sections = [
        f"# {market} Market Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source Scan: {payload.get('generated_at') or 'Unknown'}",
        f"- Total Results: {payload.get('total_results') or 0}",
        f"- Total Scanned: {payload.get('total_scanned') or 0}",
        f"- Average Score: {report_payload.get('avg_score') if report_payload else '—'}",
        "",
        "## Score Distribution",
        _render_markdown_table(
            ["Score", "Count", "Share"],
            [[item["score"], item["count"], f"{item['share']}%"] for item in (report_payload or {}).get("score_distribution") or []],
        ) if report_payload and report_payload.get("score_distribution") else "_No score data_",
        "",
        "## Top Ranked Names",
    ]
    headers_md, rows_md, _ = _build_ranked_table_data(market, (report_payload or payload).get("stocks")[:10])
    md_sections.append(_render_markdown_table(headers_md, rows_md) if rows_md else "_No ranked rows_")
    files = _export_file_bundle(
        export_root=export_root,
        base_stem=f"{market.lower()}_market_{timestamp}",
        export_format=_normalized_export_format(export_format),
        csv_headers=headers,
        csv_rows=rows,
        md_lines=md_sections,
    )

    export_payload = {
        "market": market,
        "dataset": "market",
        "format": _normalized_export_format(export_format),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": payload.get("generated_at"),
        "rows_exported": len(rows),
        "files": files,
    }
    if archive_db:
        export_payload["artifact_id"] = _record_archive_artifact(
            archive_db,
            market,
            "export",
            export_payload,
            scan_generated_at=payload.get("generated_at"),
            source_file=payload.get("_source"),
        )
    return export_payload


def _build_sector_report_export_payload(market, results_file=None, db_path=None, archive_db=None, out_dir=None, sector=None, export_format="both", limit=200):
    payload = _build_sector_report_payload(market, results_file=results_file, db_path=db_path, sector=sector, limit=max(30, limit))
    if payload is None:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_root = Path(out_dir or Path.cwd() / "exports") / market.lower()
    export_format = _normalized_export_format(export_format)
    sector_slug = (payload.get("sector_filter") or "all").lower().replace(" ", "_").replace("/", "_")

    summary_headers = ["Sector", "Count", "AvgScore", "BestScore", "NearHighCount", "AvgTurnover", "Leader"]
    summary_rows = [
        [
            item.get("sector") or "",
            item.get("count") or 0,
            item.get("avg_score") or 0,
            item.get("best_score") or 0,
            item.get("near_high_count") or 0,
            item.get("avg_turnover") or 0,
            item.get("leader") or "",
        ]
        for item in payload.get("sector_summary") or []
    ]

    if payload.get("sector_filter"):
        if market == "US":
            csv_headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverM", "MarketCapB", "Sector", "Industry", "Signals"]
            csv_rows = [
                [
                    row.get("ticker") or "",
                    row.get("name") or "",
                    row.get("score") or 0,
                    row.get("price") or "",
                    row.get("avg_turnover_m") or "",
                    row.get("market_cap_b") or "",
                    row.get("sector") or "",
                    row.get("industry") or "",
                    " | ".join(row.get("signals") or []),
                ]
                for row in payload.get("stocks") or []
            ]
        else:
            csv_headers = ["Ticker", "Name", "Score", "Price", "AvgTurnoverCr", "MarketCapCr", "CapCategory", "Sector", "Industry", "Signals"]
            csv_rows = [
                [
                    row.get("ticker") or "",
                    row.get("name") or "",
                    row.get("score") or 0,
                    row.get("price") or "",
                    row.get("avg_turnover_cr") or "",
                    row.get("market_cap_cr") or "",
                    row.get("cap_category") or "",
                    row.get("sector") or "",
                    row.get("industry") or "",
                    " | ".join(row.get("signals") or []),
                ]
                for row in payload.get("stocks") or []
            ]
    else:
        csv_headers = summary_headers
        csv_rows = summary_rows

    md_sections = [
        f"# {market} Sector Report Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Source Scan: {payload.get('scan_generated_at') or 'Unknown'}",
        f"- Requested Sector: {payload.get('sector_filter') or 'All sectors'}",
        f"- Total Results: {payload.get('total_results') or 0}",
        "",
        "## Sector Summary",
        _render_markdown_table(summary_headers, summary_rows[:20]) if summary_rows else "_No sector summary rows_",
    ]
    if payload.get("sector_filter"):
        headers_md, rows_md, _ = _build_ranked_table_data(market, payload.get("stocks") or [])
        md_sections.extend([
            "",
            f"## {payload.get('sector_filter')} Ranked Names",
            _render_markdown_table(headers_md, rows_md) if rows_md else "_No ranked rows_",
        ])

    files = _export_file_bundle(
        export_root=export_root,
        base_stem=f"{market.lower()}_sector_report_{sector_slug}_{timestamp}",
        export_format=export_format,
        csv_headers=csv_headers,
        csv_rows=csv_rows,
        md_lines=md_sections,
    )
    export_payload = {
        "market": market,
        "dataset": "sector-report",
        "format": export_format,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": payload.get("scan_generated_at"),
        "rows_exported": len(csv_rows),
        "files": files,
        "sector_filter": payload.get("sector_filter"),
    }
    if archive_db:
        export_payload["artifact_id"] = _record_archive_artifact(
            archive_db,
            market,
            "export",
            export_payload,
            scan_generated_at=payload.get("scan_generated_at"),
            source_file=payload.get("source_file"),
        )
    return export_payload


def _build_sector_history_export_payload(market, archive_db, out_dir=None, sector=None, export_format="both", limit=200):
    payload = _build_sector_history_payload(market=market, archive_db=archive_db, sector=sector, limit=max(20, limit))
    if payload is None:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_root = Path(out_dir or Path.cwd() / "exports") / market.lower()
    export_format = _normalized_export_format(export_format)
    sector_slug = (payload.get("sector_filter") or "latest_snapshot").lower().replace(" ", "_").replace("/", "_")

    if payload.get("sector_filter"):
        csv_headers = ["ScanGeneratedAt", "SectorRank", "SectorCount", "DeltaCount", "AvgScore", "DeltaAvgScore", "BestScore", "NearHighCount", "DeltaNearHigh", "AvgTurnover", "Leader"]
        csv_rows = [
            [
                row.get("scan_generated_at") or "",
                row.get("sector_rank") or "",
                row.get("sector_count") or "",
                row.get("delta_count") if row.get("delta_count") is not None else "",
                row.get("avg_score") or "",
                row.get("delta_avg_score") if row.get("delta_avg_score") is not None else "",
                row.get("best_score") or "",
                row.get("near_high_count") or "",
                row.get("delta_near_high") if row.get("delta_near_high") is not None else "",
                row.get("avg_turnover") or "",
                row.get("leader") or "",
            ]
            for row in payload.get("rows") or []
        ]
        md_headers = ["Scan", "Rank", "Cnt", "dCnt", "Avg", "dAvg", "Best", "NH", "dNH", "Avg Turn", "Leader"]
        md_rows = [
            [
                row.get("scan_generated_at") or "",
                row.get("sector_rank") or "",
                row.get("sector_count") or "",
                row.get("delta_count") if row.get("delta_count") is not None else "",
                row.get("avg_score") or "",
                row.get("delta_avg_score") if row.get("delta_avg_score") is not None else "",
                row.get("best_score") or "",
                row.get("near_high_count") or "",
                row.get("delta_near_high") if row.get("delta_near_high") is not None else "",
                row.get("avg_turnover") or "",
                row.get("leader") or "",
            ]
            for row in payload.get("rows") or []
        ]
    else:
        csv_headers = ["Sector", "SectorRank", "SectorCount", "DeltaCount", "AvgScore", "DeltaAvgScore", "BestScore", "NearHighCount", "DeltaNearHigh", "AvgTurnover", "Leader", "LatestScan"]
        csv_rows = [
            [
                row.get("sector") or "",
                row.get("sector_rank") or "",
                row.get("sector_count") or "",
                row.get("delta_count") if row.get("delta_count") is not None else "",
                row.get("avg_score") or "",
                row.get("delta_avg_score") if row.get("delta_avg_score") is not None else "",
                row.get("best_score") or "",
                row.get("near_high_count") or "",
                row.get("delta_near_high") if row.get("delta_near_high") is not None else "",
                row.get("avg_turnover") or "",
                row.get("leader") or "",
                row.get("scan_generated_at") or "",
            ]
            for row in payload.get("rows") or []
        ]
        md_headers = ["Sector", "Rank", "Cnt", "dCnt", "Avg", "dAvg", "Best", "NH", "dNH", "Avg Turn", "Leader"]
        md_rows = [
            [
                row.get("sector") or "",
                row.get("sector_rank") or "",
                row.get("sector_count") or "",
                row.get("delta_count") if row.get("delta_count") is not None else "",
                row.get("avg_score") or "",
                row.get("delta_avg_score") if row.get("delta_avg_score") is not None else "",
                row.get("best_score") or "",
                row.get("near_high_count") or "",
                row.get("delta_near_high") if row.get("delta_near_high") is not None else "",
                row.get("avg_turnover") or "",
                row.get("leader") or "",
            ]
            for row in payload.get("rows") or []
        ]

    md_sections = [
        f"# {market} Sector History Export",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Latest Scan: {payload.get('latest_scan_generated_at') or 'Unknown'}",
        f"- Previous Scan: {payload.get('previous_scan_generated_at') or 'Unknown'}",
        f"- Sector Filter: {payload.get('sector_filter') or 'Snapshot'}",
        "",
        "## Sector History",
        _render_markdown_table(md_headers, md_rows) if md_rows else "_No sector history rows_",
    ]

    files = _export_file_bundle(
        export_root=export_root,
        base_stem=f"{market.lower()}_sector_history_{sector_slug}_{timestamp}",
        export_format=export_format,
        csv_headers=csv_headers,
        csv_rows=csv_rows,
        md_lines=md_sections,
    )
    export_payload = {
        "market": market,
        "dataset": "sector-history",
        "format": export_format,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_generated_at": payload.get("latest_scan_generated_at"),
        "rows_exported": len(csv_rows),
        "files": files,
        "sector_filter": payload.get("sector_filter"),
    }
    if archive_db:
        export_payload["artifact_id"] = _record_archive_artifact(
            archive_db,
            market,
            "export",
            export_payload,
            scan_generated_at=payload.get("latest_scan_generated_at"),
            source_file="sector_history",
        )
    return export_payload


def _build_export_payload(market, results_file=None, db_path=None, archive_db=None, out_dir=None, export_format="both", dataset="market", sector=None, limit=200):
    dataset_name = (dataset or "market").strip().lower()
    if dataset_name == "sector-report":
        return _build_sector_report_export_payload(
            market=market,
            results_file=results_file,
            db_path=db_path,
            archive_db=archive_db,
            out_dir=out_dir,
            sector=sector,
            export_format=export_format,
            limit=limit,
        )
    if dataset_name == "sector-history":
        return _build_sector_history_export_payload(
            market=market,
            archive_db=archive_db or db_path,
            out_dir=out_dir,
            sector=sector,
            export_format=export_format,
            limit=limit,
        )
    return _build_market_export_payload(
        market=market,
        results_file=results_file,
        db_path=db_path,
        archive_db=archive_db,
        out_dir=out_dir,
        export_format=export_format,
    )


def _format_duration(started_at, completed_at):
    start_dt = _parse_generated_at(started_at)
    end_dt = _parse_generated_at(completed_at)
    if start_dt is None or end_dt is None or end_dt < start_dt:
        return "—"
    total_seconds = int((end_dt - start_dt).total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def _print_market_history(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    section_code = "1;96" if market == "IN" else f"1;{accent}"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), "96")
    market_label = "US" if market == "US" else "INDIA"
    db_name = os.path.basename(payload.get("db_path") or "—")
    summary = payload.get("summary") or {}
    row_palette = ["97", "96", "38;5;223", "97"] if market == "IN" else ["97", "96", "38;5;159", "97"]

    print("")
    print(rail)
    print(_style(f"{market_label} ARCHIVE HISTORY", f"1;{accent}"))
    print(_style(f"Database: {db_name}", "2"))
    print(
        _style(
            f"Events: {summary.get('total_events', 0):,}  |  "
            f"Tickers: {summary.get('unique_tickers', 0):,}  |  "
            f"Last Event: {summary.get('last_event_at') or '—'}",
            "2",
        )
    )
    print(rail)

    summary_rows = [
        ["Total Events", f"{summary.get('total_events', 0):,}"],
        ["Unique Tickers", f"{summary.get('unique_tickers', 0):,}"],
        ["First Scan Date", summary.get("first_scan_date") or "—"],
        ["Last Scan Date", summary.get("last_scan_date") or "—"],
        ["Last Event At", summary.get("last_event_at") or "—"],
    ]
    print("")
    print(_style("Summary", section_code))
    print(_paint_table(_render_table(["Metric", "Value"], summary_rows, ["left", "left"]), border_code="96", header_code=accent, row_palette=row_palette))

    recent_runs = payload.get("recent_runs") or []
    run_rows = [
        [
            _truncate(item.get("run_date") or "—", 10),
            _truncate(item.get("started_at") or "—", 19),
            _truncate(item.get("completed_at") or "—", 19),
            _truncate(item.get("status") or "—", 10),
            str(item.get("total_scanned") or 0),
            str(item.get("total_results") or 0),
            _format_duration(item.get("started_at"), item.get("completed_at")),
        ]
        for item in recent_runs
    ]
    print("")
    print(_style("Recent Runs", section_code))
    if run_rows:
        print(
            _paint_table(
                _render_table(
                    ["Run Date", "Started", "Completed", "Status", "Scanned", "Results", "Duration"],
                    run_rows,
                    ["left", "left", "left", "left", "right", "right", "right"],
                ),
                border_code="96",
                header_code=accent,
                row_palette=row_palette,
            )
        )
    else:
        print(_style("No archived runs found.", "33"))

    event_rows = [
        [
            _truncate(item.get("scanned_at") or item.get("scan_date") or "—", 19),
            _truncate(item.get("ticker") or "—", 12),
            str(item.get("score") or 0),
            _truncate(item.get("failed") or "—", 18),
            _truncate(item.get("source") or "—", 14),
        ]
        for item in payload.get("recent_events") or []
    ]
    print("")
    print(_style("Recent Events", section_code))
    if event_rows:
        print(
            _paint_table(
                _render_table(
                    ["When", "Ticker", "Score", "Failed", "Source"],
                    event_rows,
                    ["left", "left", "right", "left", "left"],
                ),
                border_code="96",
                header_code=accent,
                row_palette=row_palette,
            )
        )
    else:
        print(_style("No archived events found.", "33"))

    ticker_rows = [
        [
            _truncate(item.get("ticker") or "—", 12),
            str(item.get("scans") or 0),
            str(item.get("best_score") or 0),
            _truncate(item.get("last_scan_date") or "—", 10),
        ]
        for item in payload.get("most_scanned_tickers") or []
    ]
    print("")
    print(_style("Most Scanned Tickers", section_code))
    if ticker_rows:
        print(
            _paint_table(
                _render_table(
                    ["Ticker", "Scans", "Best", "Last Scan"],
                    ticker_rows,
                    ["left", "right", "right", "left"],
                ),
                border_code="96",
                header_code=accent,
                row_palette=row_palette,
            )
        )
    else:
        print(_style("No ticker history found.", "33"))

    snapshot_rows = [
        [
            _truncate(item.get("snapshot_date") or "—", 10),
            _truncate(item.get("created_at") or "—", 19),
            _truncate(os.path.basename(item.get("file_path") or "—"), 42),
        ]
        for item in payload.get("recent_snapshots") or []
    ]
    print("")
    print(_style("Recent Snapshots", section_code))
    if snapshot_rows:
        print(
            _paint_table(
                _render_table(
                    ["Snapshot", "Created", "File"],
                    snapshot_rows,
                    ["left", "left", "left"],
                ),
                border_code="96",
                header_code=accent,
                row_palette=row_palette,
            )
        )
    else:
        print(_style("No snapshots stored yet.", "33"))

    print(rail)
    print("")


def _print_artifact_history(payload):
    scope = payload.get("market_scope") or "ALL"
    accent = "96" if scope == "US" else ("38;5;214" if scope == "IN" else "97")
    label = f"{scope} ARTIFACT HISTORY" if scope != "ALL" else "CROSS-MARKET ARTIFACT HISTORY"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}", "2"))
    artifact_filter = payload.get("artifact_filter") or "all"
    print(_style(f"Filter: {artifact_filter}  |  Stored artifacts: {payload.get('artifact_count') or 0}", "2"))
    print(rail)

    summary_rows = [
        [
            _truncate(item["artifact_type"], 18),
            str(item["count"]),
            _truncate(item["latest_generated_at"] or "—", 19),
            _truncate(item["markets"] or "—", 10),
            _truncate(item["note"] or "—", 38),
        ]
        for item in payload.get("summary_rows") or []
    ]
    if summary_rows:
        print("")
        print(_style("Artifact Types", f"1;{accent}"))
        print(_render_table(["Type", "Count", "Latest", "Markets", "Latest Note"], summary_rows, ["left", "right", "left", "left", "left"]))

    recent_rows = [
        [
            str(item["id"]),
            _truncate(item["generated_at"] or "—", 19),
            _truncate(item["market"] or "—", 4),
            _truncate(item["artifact_type"] or "—", 18),
            _truncate(item.get("scan_generated_at") or "—", 19),
            _truncate(item.get("reference_date") or "—", 10),
            _truncate(os.path.basename(item.get("source_file") or "—"), 24),
            _truncate(item.get("note") or "—", 34),
        ]
        for item in payload.get("recent_artifacts") or []
    ]
    print("")
    print(_style("Recent Artifacts", f"1;{accent}"))
    if recent_rows:
        print(
            _render_table(
                ["ID", "Generated", "Mkt", "Type", "Scan", "Ref", "Source", "Note"],
                recent_rows,
                ["right", "left", "left", "left", "left", "left", "left", "left"],
            )
        )
    else:
        print(_style("No archived artifacts found.", "33"))
    print(rail)
    print("")


def _print_archive_query(payload):
    scope = payload.get("market_scope") or "ALL"
    accent = "96" if scope == "US" else ("38;5;214" if scope == "IN" else "97")
    label = f"{scope} ARCHIVE QUERY" if scope != "ALL" else "CROSS-MARKET ARCHIVE QUERY"
    rail = _style("═" * max(104, min(shutil.get_terminal_size((132, 24)).columns, 150)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    command_filter = payload.get("command_filter") or "all"
    search_filter = payload.get("search_filter") or "—"
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}", "2"))
    print(_style(f"Command: {command_filter}  |  Search: {search_filter}  |  Matches: {payload.get('matched_count') or 0}", "2"))
    print(rail)

    rows = [
        [
            str(item["id"]),
            _truncate(item.get("generated_at") or "—", 19),
            _truncate(item.get("market") or "—", 4),
            _truncate(item.get("artifact_type") or "—", 16),
            _truncate(item.get("entity") or "—", 14),
            _truncate(item.get("scan_generated_at") or "—", 19),
            _truncate(os.path.basename(item.get("source_file") or "—"), 22),
            _truncate(item.get("note") or "—", 36),
        ]
        for item in payload.get("rows") or []
    ]
    if rows:
        print(_render_table(["ID", "Generated", "Mkt", "Command", "Entity", "Scan", "Source", "Note"], rows, ["right", "left", "left", "left", "left", "left", "left", "left"]))
    else:
        print(_style("No archive rows matched the requested filters.", "33"))
    print(rail)
    print("")


def _print_ticker_history(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    label = f"{market} TICKER HISTORY"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)
    profile = payload.get("profile") or {}

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"{payload.get('ticker') or '—'}  |  {profile.get('name') or payload.get('ticker') or '—'}", "1"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}", "2"))
    print(_style(f"Sector: {profile.get('sector') or '—'}  |  Industry: {profile.get('industry') or '—'}", "2"))
    print(rail)
    print(
        f"Latest: {payload.get('latest_score') or 0}  |  "
        f"Best: {payload.get('best_score') or 0}  |  "
        f"Avg: {payload.get('avg_score') or 0}  |  "
        f"Scans: {payload.get('total_scans') or 0}  |  "
        f"Score>=5 Hits: {payload.get('high_score_hits') or 0}  |  "
        f"Near 52W High Hits: {payload.get('near_high_hits') or 0}"
    )
    print(
        f"Price: {_format_price(profile.get('price'), '$' if market == 'US' else '₹')}  |  "
        f"Turnover: {_format_metric(profile.get('turnover'), _market_turnover_label(market), 1)}  |  "
        f"Market Cap: {_format_metric(profile.get('market_cap'), 'B' if market == 'US' else 'Cr', 3 if market == 'US' else 0)}"
    )
    print(
        f"First Seen: {payload.get('first_seen') or '—'}  |  "
        f"Last Scan: {payload.get('last_scanned_at') or '—'}"
    )
    signals_text = " · ".join(profile.get("signals") or []) or "—"
    print(_style(f"Latest Signals: {_truncate(signals_text, 110)}", "2"))

    event_rows = [
        [
            _truncate(item.get("display_when") or item.get("scanned_at") or item.get("scan_date") or "—", 19),
            str(item.get("score") or 0),
            _truncate(item.get("failed") or "—", 16),
            _truncate(item.get("source") or "—", 12),
            _truncate(item.get("signals") or "—", 58),
        ]
        for item in payload.get("recent_events") or []
    ]
    print("")
    print(_style("Recent Scan Events", f"1;{accent}"))
    if event_rows:
        print(_render_table(["When", "Score", "Failed", "Source", "Signals"], event_rows, ["left", "right", "left", "left", "left"]))
    else:
        print(_style("No scan events stored for this ticker.", "33"))
    print(rail)
    print("")


def _history_hit_rows(rows, metric_key, value_label):
    output = []
    for row in rows:
        age = _format_age_from_datetime(_parse_generated_at(row.get("last_scanned_at")))
        output.append(
            [
                _truncate(row.get("ticker") or "—", 10),
                _truncate(row.get("name") or "—", 24),
                str(row.get(metric_key) or 0),
                str(row.get("best_score") or 0),
                str(row.get("scans") or 0),
                age,
            ]
        )
    return output, ["Ticker", "Name", value_label, "Best", "Scans", "Last Seen"], ["left", "left", "right", "right", "right", "right"]


def _print_leaderboard(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    label = f"{market} LEADERBOARD"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}  |  Latest scan: {payload.get('scan_generated_at') or 'Unknown'}", "2"))
    print(
        _style(
            f"Current results: {payload.get('current_total') or 0}  |  "
            f"History events: {payload.get('history_events') or 0}  |  "
            f"Unique tickers: {payload.get('unique_tickers') or 0}",
            "2",
        )
    )
    print(rail)

    print("")
    print(_style("Current Leaders", f"1;{accent}"))
    headers, rows, aligns = _build_ranked_table_data(market, payload.get("current_leaders") or [])
    if rows:
        print(_render_table(headers, rows, aligns))
    else:
        print(_style("No current ranked rows available.", "33"))

    print("")
    print(_style("Most Consistent Leaders", f"1;{accent}"))
    consistent_rows, consistent_headers, consistent_aligns = _history_hit_rows(payload.get("consistent_leaders") or [], "avg_score", "Avg")
    if consistent_rows:
        print(_render_table(consistent_headers, consistent_rows, consistent_aligns))
    else:
        print(_style("Not enough historical scans to rank consistency yet.", "33"))

    print("")
    print(_style("Most Score>=5 Hits", f"1;{accent}"))
    hit_rows, hit_headers, hit_aligns = _history_hit_rows(payload.get("high_score_hits") or [], "hits", "Hits")
    if hit_rows:
        print(_render_table(hit_headers, hit_rows, hit_aligns))
    else:
        print(_style("No score>=5 history stored yet.", "33"))

    print("")
    print(_style("Most Near 52W High Hits", f"1;{accent}"))
    near_rows, near_headers, near_aligns = _history_hit_rows(payload.get("near_high_hits") or [], "hits", "Hits")
    if near_rows:
        print(_render_table(near_headers, near_rows, near_aligns))
    else:
        print(_style("No Near 52W High history stored yet.", "33"))
    print(rail)
    print("")


def _print_sector_report(payload, market, limit):
    accent = "96" if market == "US" else "38;5;214"
    label = f"{market} SECTOR REPORT"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}  |  Latest scan: {payload.get('scan_generated_at') or 'Unknown'}", "2"))
    print(_style(f"Source: {payload.get('source_file') or 'unknown'}  |  Total results: {payload.get('total_results') or 0}", "2"))
    print(rail)

    summary_rows = [
        [
            _truncate(item["sector"], 26),
            str(item["count"]),
            _format_number(item["avg_score"], 2),
            str(item["best_score"]),
            str(item["near_high_count"]),
            _format_metric(item["avg_turnover"], _market_turnover_label(market), 1),
            _truncate(item["leader"], 10),
        ]
        for item in (payload.get("sector_summary") or [])[:20]
    ]
    print("")
    print(_style("Sector Summary", f"1;{accent}"))
    if summary_rows:
        print(_render_table(["Sector", "Count", "Avg", "Best", "NearHigh", "Avg Turn", "Leader"], summary_rows, ["left", "right", "right", "right", "right", "right", "left"]))
    else:
        print(_style("No sector data available in the latest results.", "33"))

    sector_filter = payload.get("sector_filter")
    if sector_filter:
        print("")
        print(_style(f"{sector_filter} Ranked Table", f"1;{accent}"))
        sector_rows = payload.get("stocks") or []
        print(_style(f"Showing top {min(limit, len(sector_rows))} of {payload.get('filtered_count') or 0} rows in this sector.", "2"))
        if sector_rows:
            headers, rows, aligns = _build_ranked_table_data(market, sector_rows)
            print(_render_table(headers, rows, aligns))
        else:
            print(_style("No rows matched that sector filter.", "33"))
    print(rail)
    print("")


def _format_signed_delta(value, decimals=2, suffix=""):
    if value is None:
        return "—"
    number = float(value)
    if decimals == 0:
        text = f"{int(round(number)):+d}"
    else:
        text = f"{number:+.{decimals}f}"
    return f"{text}{suffix}"


def _print_sector_history(payload, market, limit):
    accent = "96" if market == "US" else "38;5;214"
    label = f"{market} SECTOR HISTORY"
    rail = _style("═" * max(104, min(shutil.get_terminal_size((132, 24)).columns, 150)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}", "2"))
    if payload.get("sector_filter"):
        print(
            _style(
                f"Sector: {payload.get('sector_filter')}  |  Rows: {payload.get('matched_count') or 0}  |  "
                f"Latest scan: {payload.get('latest_scan_generated_at') or '—'}",
                "2",
            )
        )
    else:
        print(
            _style(
                f"Latest scan: {payload.get('latest_scan_generated_at') or '—'}  |  "
                f"Previous scan: {payload.get('previous_scan_generated_at') or '—'}  |  "
                f"Sectors shown: {payload.get('matched_count') or 0}",
                "2",
            )
        )
    print(rail)

    rows = payload.get("rows") or []
    if not rows:
        print(_style("No sector history rows found for the requested filters.", "33"))
        print(rail)
        print("")
        return

    if payload.get("sector_filter"):
        table_rows = [
            [
                _truncate(item.get("scan_generated_at") or "—", 19),
                _align(item.get("sector_rank") or "—", 4, "right"),
                _align(item.get("sector_count") or "—", 5, "right"),
                _align(_format_signed_delta(item.get("delta_count"), 0), 6, "right"),
                _align(_format_number(item.get("avg_score"), 2), 5, "right"),
                _align(_format_signed_delta(item.get("delta_avg_score"), 2), 7, "right"),
                _align(item.get("best_score") or "—", 4, "right"),
                _align(item.get("near_high_count") or "—", 4, "right"),
                _align(_format_signed_delta(item.get("delta_near_high"), 0), 6, "right"),
                _align(_format_metric(item.get("avg_turnover"), _market_turnover_label(market), 1), 10, "right"),
                _truncate(item.get("leader") or "—", 12),
            ]
            for item in rows[:limit]
        ]
        headers = ["Scan", "Rank", "Cnt", "ΔCnt", "Avg", "ΔAvg", "Best", "NH", "ΔNH", "Avg Turn", "Leader"]
        aligns = ["left", "right", "right", "right", "right", "right", "right", "right", "right", "right", "left"]
    else:
        table_rows = [
            [
                _truncate(item.get("sector") or "—", 24),
                _align(item.get("sector_rank") or "—", 4, "right"),
                _align(item.get("sector_count") or "—", 5, "right"),
                _align(_format_signed_delta(item.get("delta_count"), 0), 6, "right"),
                _align(_format_number(item.get("avg_score"), 2), 5, "right"),
                _align(_format_signed_delta(item.get("delta_avg_score"), 2), 7, "right"),
                _align(item.get("best_score") or "—", 4, "right"),
                _align(item.get("near_high_count") or "—", 4, "right"),
                _align(_format_signed_delta(item.get("delta_near_high"), 0), 6, "right"),
                _align(_format_metric(item.get("avg_turnover"), _market_turnover_label(market), 1), 10, "right"),
                _truncate(item.get("leader") or "—", 12),
            ]
            for item in rows[:limit]
        ]
        headers = ["Sector", "Rank", "Cnt", "ΔCnt", "Avg", "ΔAvg", "Best", "NH", "ΔNH", "Avg Turn", "Leader"]
        aligns = ["left", "right", "right", "right", "right", "right", "right", "right", "right", "right", "left"]

    print(_render_table(headers, table_rows, aligns))
    print(rail)
    print("")


def _print_new_highs(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    label = f"{market} NEAR 52W HIGH"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)

    print("")
    print(rail)
    print(_style(label, f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}  |  Latest scan: {payload.get('scan_generated_at') or 'Unknown'}", "2"))
    print(_style(f"Total qualified: {payload.get('total_results') or 0}  |  Near 52W High: {payload.get('new_high_count') or 0}  |  Best score: {payload.get('top_score') or 0}", "2"))
    print(rail)

    sector_rows = [
        [
            _truncate(item["sector"], 24),
            str(item["count"]),
            _format_number(item["avg_score"], 2),
            str(item["best_score"]),
            _truncate(item["leader"], 10),
        ]
        for item in (payload.get("sector_summary") or [])[:10]
    ]
    print("")
    print(_style("Sector Breakdown", f"1;{accent}"))
    if sector_rows:
        print(_render_table(["Sector", "Count", "Avg", "Best", "Leader"], sector_rows, ["left", "right", "right", "right", "left"]))
    else:
        print(_style("No Near 52W High names found in the latest results.", "33"))

    print("")
    print(_style("Ranked Names", f"1;{accent}"))
    headers, rows, aligns = _build_ranked_table_data(market, payload.get("stocks") or [])
    if rows:
        print(_render_table(headers, rows, aligns))
    else:
        print(_style("No rows to display.", "33"))
    print(rail)
    print("")


def _print_compare_markets(payload):
    accent = "97"
    rail = _style("═" * max(108, min(shutil.get_terminal_size((140, 24)).columns, 150)), accent)
    print("")
    print(rail)
    print(_style("US VS INDIA MARKET COMPARE", f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}", "2"))
    print(rail)

    markets = payload.get("markets") or {}
    summary_rows = []
    for market in ("US", "IN"):
        item = markets.get(market) or {}
        summary_rows.append(
            [
                market,
                _truncate(item.get("generated_at") or "—", 19),
                str(item.get("total_scanned") or 0),
                str(item.get("total_results") or 0),
                _format_number(item.get("avg_score"), 2),
                str(item.get("top_score") or 0),
                str(item.get("score_ge_5") or 0),
                str(item.get("near_high_count") or 0),
                str(item.get("volume_spike_count") or 0),
                _truncate(item.get("top_sector") or "—", 18),
                _truncate(item.get("leader") or "—", 10),
            ]
        )
    print(_render_table(["Mkt", "Scan", "Scanned", "Results", "Avg", "Best", "Score>=5", "NearHigh", "VolSpike", "Top Sector", "Leader"], summary_rows, ["left", "left", "right", "right", "right", "right", "right", "right", "right", "left", "left"]))

    for market in ("US", "IN"):
        item = markets.get(market) or {}
        section_accent = "96" if market == "US" else "38;5;214"
        print("")
        print(_style(f"{market} Sector Leaders", f"1;{section_accent}"))
        sector_rows = [
            [
                _truncate(row["sector"], 24),
                str(row["count"]),
                _format_number(row["avg_score"], 2),
                str(row["best_score"]),
                _truncate(row["leader"], 10),
            ]
            for row in (item.get("sector_summary") or [])[:6]
        ]
        if sector_rows:
            print(_render_table(["Sector", "Count", "Avg", "Best", "Leader"], sector_rows, ["left", "right", "right", "right", "left"]))
        else:
            print(_style("No sector rows available.", "33"))

        print("")
        print(_style(f"{market} Top Ranked", f"1;{section_accent}"))
        headers, rows, aligns = _build_ranked_table_data(market, item.get("top_rows") or [])
        if rows:
            print(_render_table(headers, rows, aligns))
        else:
            print(_style("No ranked rows available.", "33"))

    print(rail)
    print("")


def _print_doctor(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)
    print("")
    print(rail)
    print(_style(f"{market} SCANNER DOCTOR", f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}  |  Overall: {payload.get('overall_status') or 'UNKNOWN'}", "2"))
    print(_style(f"Warnings: {payload.get('warning_count') or 0}  |  Failures: {payload.get('fail_count') or 0}", "2"))
    print(rail)

    rows = [
        [
            _truncate(item["name"], 16),
            _truncate(item["status"], 6),
            _truncate(item["details"], 86),
        ]
        for item in payload.get("checks") or []
    ]
    if rows:
        print(_render_table(["Check", "Status", "Details"], rows, ["left", "left", "left"]))
    else:
        print(_style("No health checks were produced.", "33"))
    print(rail)
    print("")


def _print_export(payload, market):
    accent = "96" if market == "US" else "38;5;214"
    rail = _style("═" * max(96, min(shutil.get_terminal_size((120, 24)).columns, 140)), accent)
    print("")
    print(rail)
    print(_style(f"{market} EXPORT COMPLETE", f"1;{accent}"))
    print(_style(f"Generated: {payload.get('generated_at') or 'Unknown'}  |  Source scan: {payload.get('scan_generated_at') or 'Unknown'}", "2"))
    print(
        _style(
            f"Dataset: {payload.get('dataset') or 'market'}  |  "
            f"Format: {payload.get('format') or 'both'}  |  "
            f"Rows exported: {payload.get('rows_exported') or 0}  |  "
            f"Artifact ID: {payload.get('artifact_id') or '—'}",
            "2",
        )
    )
    print(rail)
    file_rows = [
        [
            _truncate(item.get("kind") or "—", 12),
            item.get("path") or "—",
        ]
        for item in payload.get("files") or []
    ]
    if file_rows:
        print(_render_table(["Kind", "Path"], file_rows, ["left", "left"]))
    else:
        print(_style("No export files were created.", "33"))
    print(rail)
    print("")


def _run_cli():
    parser = argparse.ArgumentParser(description="Manage scan state SQLite databases.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reset_parser = subparsers.add_parser("reset", help="Reset one market from a scan state DB.")
    reset_parser.add_argument("--db", required=True, help="Path to the SQLite database.")
    reset_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to reset.")

    history_parser = subparsers.add_parser("history", help="Show archived history for one market.")
    history_parser.add_argument("--db", required=True, help="Path to the SQLite database.")
    history_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    history_parser.add_argument("--events", type=int, default=10, help="Recent event rows to include.")
    history_parser.add_argument("--runs", type=int, default=5, help="Recent run rows to include.")
    history_parser.add_argument("--snapshots", type=int, default=5, help="Recent snapshot rows to include.")
    history_parser.add_argument("--tickers", type=int, default=10, help="Most-scanned tickers to include.")
    history_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the history output should be stored.")
    history_parser.add_argument("--json", action="store_true", help="Print raw JSON instead of the formatted terminal view.")

    summary_parser = subparsers.add_parser("summary", help="Show the latest scan summary table for one market.")
    summary_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    summary_parser.add_argument("--results-file", help="Current results JSON file to summarize.")
    summary_parser.add_argument("--db", help="Fallback SQLite database if the results JSON file is missing.")
    summary_parser.add_argument("--limit", type=int, default=30, help="Maximum number of rows to show.")
    summary_parser.add_argument("--cap-class", choices=("smallcap", "midcap", "largecap"), help="Optional India-only cap class filter.")
    summary_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the summary output should be stored.")

    diff_parser = subparsers.add_parser("diff", help="Compare the latest results against the newest older snapshot.")
    diff_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    diff_parser.add_argument("--results-file", required=True, help="Current results JSON file.")
    diff_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the diff artifact should be stored.")
    diff_parser.add_argument("--limit", type=int, default=15, help="Maximum rows per diff section.")

    report_parser = subparsers.add_parser("report", help="Generate a richer latest-scan report.")
    report_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    report_parser.add_argument("--results-file", help="Current results JSON file.")
    report_parser.add_argument("--db", help="Fallback SQLite database if the results JSON file is missing.")
    report_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the report artifact should be stored.")
    report_parser.add_argument("--limit", type=int, default=30, help="Maximum number of top rows to show.")

    artifact_history_parser = subparsers.add_parser("artifact-history", help="Show archived generated outputs for one market.")
    artifact_history_parser.add_argument("--db", required=True, help="Permanent SQLite archive DB.")
    artifact_history_parser.add_argument("--market", required=True, choices=("US", "IN", "ALL"), help="Artifact market scope.")
    artifact_history_parser.add_argument("--artifact-type", help="Optional artifact type filter.")
    artifact_history_parser.add_argument("--limit", type=int, default=20, help="Maximum recent artifact rows to show.")

    archive_query_parser = subparsers.add_parser("archive-query", help="Query archived command-output rows directly.")
    archive_query_parser.add_argument("--db", required=True, help="Permanent SQLite archive DB.")
    archive_query_parser.add_argument("--market", required=True, choices=("US", "IN", "ALL"), help="Archive market scope.")
    archive_query_parser.add_argument("--command-name", help="Optional command filter.")
    archive_query_parser.add_argument("--search", help="Optional text search across note, source, entity, and payload.")
    archive_query_parser.add_argument("--limit", type=int, default=25, help="Maximum matching rows to show.")

    ticker_history_parser = subparsers.add_parser("ticker-history", help="Show one ticker's archived score timeline.")
    ticker_history_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    ticker_history_parser.add_argument("--db", required=True, help="Permanent SQLite archive DB.")
    ticker_history_parser.add_argument("--ticker", required=True, help="Ticker/symbol to inspect.")
    ticker_history_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")
    ticker_history_parser.add_argument("--limit", type=int, default=20, help="Maximum recent events to show.")

    leaderboard_parser = subparsers.add_parser("leaderboard", help="Show historical leaderboard views for one market.")
    leaderboard_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    leaderboard_parser.add_argument("--results-file", help="Current results JSON file.")
    leaderboard_parser.add_argument("--db", help="Fallback SQLite DB for latest results.")
    leaderboard_parser.add_argument("--archive-db", required=True, help="Permanent SQLite archive DB for history and artifact storage.")
    leaderboard_parser.add_argument("--limit", type=int, default=15, help="Maximum rows per leaderboard section.")

    sector_report_parser = subparsers.add_parser("sector-report", help="Show sector summary and optional filtered sector table.")
    sector_report_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    sector_report_parser.add_argument("--results-file", help="Current results JSON file.")
    sector_report_parser.add_argument("--db", help="Fallback SQLite DB if the results JSON file is missing.")
    sector_report_parser.add_argument("--sector", help="Optional sector filter.")
    sector_report_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")
    sector_report_parser.add_argument("--limit", type=int, default=30, help="Maximum rows to show in the ranked table.")

    sector_history_parser = subparsers.add_parser("sector-history", help="Show normalized sector history over time from the archive DB.")
    sector_history_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    sector_history_parser.add_argument("--db", required=True, help="Permanent SQLite archive DB.")
    sector_history_parser.add_argument("--sector", help="Optional sector filter.")
    sector_history_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")
    sector_history_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to show.")

    new_highs_parser = subparsers.add_parser("new-highs", help="Show names near 52W highs in the latest results.")
    new_highs_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    new_highs_parser.add_argument("--results-file", help="Current results JSON file.")
    new_highs_parser.add_argument("--db", help="Fallback SQLite DB if the results JSON file is missing.")
    new_highs_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")
    new_highs_parser.add_argument("--limit", type=int, default=30, help="Maximum ranked rows to show.")

    compare_parser = subparsers.add_parser("compare-markets", help="Compare latest US and India market outputs.")
    compare_parser.add_argument("--us-results-file", help="US results JSON file.")
    compare_parser.add_argument("--india-results-file", help="India results JSON file.")
    compare_parser.add_argument("--db", help="Fallback SQLite DB if result JSON files are missing.")
    compare_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")

    doctor_parser = subparsers.add_parser("doctor", help="Run health checks for one market setup.")
    doctor_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    doctor_parser.add_argument("--results-file", help="Current results JSON file.")
    doctor_parser.add_argument("--state-db", help="Working scan state DB.")
    doctor_parser.add_argument("--archive-db", help="Permanent SQLite archive DB.")
    doctor_parser.add_argument("--venv-dir", help="Virtual environment directory.")
    doctor_parser.add_argument("--scanner-file", help="Scanner Python file.")
    doctor_parser.add_argument("--requirements-file", help="Requirements file path.")

    export_parser = subparsers.add_parser("export", help="Export market, sector report, or sector history views to CSV/Markdown.")
    export_parser.add_argument("--market", required=True, choices=("US", "IN"), help="Market to inspect.")
    export_parser.add_argument("--results-file", help="Current results JSON file.")
    export_parser.add_argument("--db", help="Fallback SQLite DB if the results JSON file is missing.")
    export_parser.add_argument("--archive-db", help="Permanent SQLite archive DB where the artifact should be stored.")
    export_parser.add_argument("--out-dir", help="Output directory for exported files.")
    export_parser.add_argument("--format", default="both", choices=("csv", "md", "both", ".csv", ".md", "markdown"), help="Export format.")
    export_parser.add_argument("--dataset", default="market", choices=("market", "sector-report", "sector-history"), help="Dataset/view to export.")
    export_parser.add_argument("--sector", help="Optional sector filter for sector-report or sector-history exports.")
    export_parser.add_argument("--limit", type=int, default=200, help="Maximum rows to export for filtered datasets.")

    workflow_export_parser = subparsers.add_parser("workflow-export", help="Export the views produced by the daily or daily-full workflows.")
    workflow_export_parser.add_argument("--workflow", required=True, choices=("daily", "daily-full"), help="Workflow to export.")
    workflow_export_parser.add_argument("--origin-market", required=True, choices=("US", "IN"), help="Launcher/market that initiated the workflow.")
    workflow_export_parser.add_argument("--us-results-file", help="US results JSON file.")
    workflow_export_parser.add_argument("--india-results-file", help="India results JSON file.")
    workflow_export_parser.add_argument("--db", help="Working SQLite DB for fallback/latest data.")
    workflow_export_parser.add_argument("--archive-db", help="Permanent archive SQLite DB.")
    workflow_export_parser.add_argument("--out-dir", help="Root output directory for workflow bundles.")
    workflow_export_parser.add_argument("--limit", type=int, default=30, help="Maximum rows per ranked export table.")
    workflow_export_parser.add_argument("--us-venv-dir", help="US virtual environment directory.")
    workflow_export_parser.add_argument("--india-venv-dir", help="India virtual environment directory.")
    workflow_export_parser.add_argument("--us-scanner-file", help="US scanner file.")
    workflow_export_parser.add_argument("--india-scanner-file", help="India scanner file.")
    workflow_export_parser.add_argument("--us-requirements-file", help="US requirements file.")

    args = parser.parse_args()

    if args.command == "reset":
        store = ScanStateStore(args.db, market=args.market)
        counts = store.reset_market()
        db_empty = not store.has_any_data()
        store.close()
        if db_empty:
            _delete_sqlite_files(args.db)
        print(
            json.dumps(
                {
                    "db": args.db,
                    "market": args.market,
                    "deleted": counts,
                    "db_removed": db_empty,
                }
            )
        )
    elif args.command == "history":
        store = ScanStateStore(args.db, market=args.market)
        summary = store.summarize_market_history(
            event_limit=args.events,
            run_limit=args.runs,
            snapshot_limit=args.snapshots,
            ticker_limit=args.tickers,
        )
        store.close()
        archive_target = args.archive_db or args.db
        if archive_target:
            summary["artifact_id"] = _record_archive_artifact(
                archive_target,
                args.market,
                "history",
                summary,
                scan_generated_at=(summary.get("summary") or {}).get("last_event_at"),
                source_file=os.path.basename(args.db),
            )
        if args.json:
            print(json.dumps(summary, indent=2))
        else:
            _print_market_history(summary, args.market)
    elif args.command == "summary":
        if not args.results_file and not args.db:
            parser.error("summary requires --results-file or --db")
        payload = _build_summary_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            limit=max(1, args.limit),
            cap_class=args.cap_class,
        )
        if payload is None:
            print(
                f"No scan results found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and database {args.db or 'N/A'}."
            )
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "summary",
                payload,
                scan_generated_at=payload.get("generated_at"),
                source_file=payload.get("_source"),
            )
        _print_summary_table(payload, args.market, max(1, args.limit))
    elif args.command == "diff":
        payload = _build_diff_payload(
            market=args.market,
            results_file=args.results_file,
            archive_db=args.archive_db,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(f"No current results found for market {args.market} at {args.results_file}.")
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "diff",
                payload,
                scan_generated_at=payload.get("scan_generated_at"),
                source_file=payload.get("source_file"),
                reference_file=payload.get("reference_file"),
                reference_date=payload.get("reference_date"),
            )
        _print_diff(payload, args.market)
    elif args.command == "report":
        if not args.results_file and not args.db:
            parser.error("report requires --results-file or --db")
        payload = _build_report_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(
                f"No scan results found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and database {args.db or 'N/A'}."
            )
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "report",
                payload,
                scan_generated_at=payload.get("generated_at"),
                source_file=payload.get("_source"),
            )
        _print_report(payload, args.market, max(1, args.limit))
    elif args.command == "artifact-history":
        payload = _build_artifact_history_payload(
            db_path=args.db,
            market=args.market,
            artifact_type=args.artifact_type,
            limit=max(1, args.limit),
            include_shared=args.market != "ALL",
        )
        if payload is None:
            print(f"No archive DB found at {args.db}.")
            raise SystemExit(1)
        artifact_market = args.market if args.market in ("US", "IN") else "ALL"
        payload["artifact_id"] = _record_archive_artifact(
            args.db,
            artifact_market,
            "artifact-history",
            payload,
        )
        _print_artifact_history(payload)
    elif args.command == "archive-query":
        payload = _build_archive_query_payload(
            db_path=args.db,
            market=args.market,
            command_name=args.command_name,
            search=args.search,
            limit=max(1, args.limit),
            include_shared=args.market != "ALL",
        )
        if payload is None:
            print(f"No archive DB found at {args.db}.")
            raise SystemExit(1)
        archive_market = args.market if args.market in ("US", "IN") else "ALL"
        payload["artifact_id"] = _record_archive_artifact(
            args.db,
            archive_market,
            "archive-query",
            payload,
        )
        _print_archive_query(payload)
    elif args.command == "ticker-history":
        payload = _build_ticker_history_payload(
            market=args.market,
            db_path=args.db,
            ticker=args.ticker,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(f"No archived history found for {args.market}:{args.ticker} in {args.db}.")
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "ticker-history",
                payload,
                scan_generated_at=payload.get("last_scanned_at"),
                source_file=args.ticker,
            )
        _print_ticker_history(payload, args.market)
    elif args.command == "leaderboard":
        if not args.results_file and not args.db:
            parser.error("leaderboard requires --results-file or --db")
        payload = _build_leaderboard_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            history_db=args.archive_db,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(
                f"No leaderboard data found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and archive DB {args.archive_db}."
            )
            raise SystemExit(1)
        payload["artifact_id"] = _record_archive_artifact(
            args.archive_db,
            args.market,
            "leaderboard",
            payload,
            scan_generated_at=payload.get("scan_generated_at"),
            source_file=args.results_file,
        )
        _print_leaderboard(payload, args.market)
    elif args.command == "sector-report":
        if not args.results_file and not args.db:
            parser.error("sector-report requires --results-file or --db")
        payload = _build_sector_report_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            sector=args.sector,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(
                f"No sector report data found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and database {args.db or 'N/A'}."
            )
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "sector-report",
                payload,
                scan_generated_at=payload.get("scan_generated_at"),
                source_file=payload.get("source_file"),
            )
        _print_sector_report(payload, args.market, max(1, args.limit))
    elif args.command == "sector-history":
        payload = _build_sector_history_payload(
            market=args.market,
            archive_db=args.db,
            sector=args.sector,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(f"No archive DB found at {args.db}.")
            raise SystemExit(1)
        archive_target = args.archive_db or args.db
        if archive_target:
            payload["artifact_id"] = _record_archive_artifact(
                archive_target,
                args.market,
                "sector-history",
                payload,
                scan_generated_at=payload.get("latest_scan_generated_at"),
                source_file=os.path.basename(args.db),
            )
        _print_sector_history(payload, args.market, max(1, args.limit))
    elif args.command == "new-highs":
        if not args.results_file and not args.db:
            parser.error("new-highs requires --results-file or --db")
        payload = _build_new_highs_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(
                f"No new-high data found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and database {args.db or 'N/A'}."
            )
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "new-highs",
                payload,
                scan_generated_at=payload.get("scan_generated_at"),
                source_file=payload.get("source_file"),
            )
        _print_new_highs(payload, args.market)
    elif args.command == "compare-markets":
        payload = _build_compare_markets_payload(
            us_results_file=args.us_results_file,
            india_results_file=args.india_results_file,
            db_path=args.db,
        )
        if payload is None:
            print("Could not load both US and India latest results for comparison.")
            raise SystemExit(1)
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                "ALL",
                "compare-markets",
                payload,
            )
        _print_compare_markets(payload)
    elif args.command == "doctor":
        payload = _build_doctor_payload(
            market=args.market,
            results_file=args.results_file,
            state_db=args.state_db,
            archive_db=args.archive_db,
            venv_dir=args.venv_dir,
            scanner_file=args.scanner_file,
            requirements_file=args.requirements_file,
        )
        if args.archive_db:
            payload["artifact_id"] = _record_archive_artifact(
                args.archive_db,
                args.market,
                "doctor",
                payload,
                scan_generated_at=payload.get("generated_at"),
                source_file=args.results_file,
            )
        _print_doctor(payload, args.market)
    elif args.command == "export":
        if args.dataset in ("market", "sector-report") and not args.results_file and not args.db:
            parser.error("export requires --results-file or --db for market and sector-report datasets")
        if args.dataset == "sector-history" and not (args.archive_db or args.db):
            parser.error("export with dataset sector-history requires --archive-db or --db")
        payload = _build_export_payload(
            market=args.market,
            results_file=args.results_file,
            db_path=args.db,
            archive_db=args.archive_db,
            out_dir=args.out_dir,
            export_format=args.format,
            dataset=args.dataset,
            sector=args.sector,
            limit=max(1, args.limit),
        )
        if payload is None:
            print(
                f"No export data found for market {args.market}. "
                f"Checked results file {args.results_file or 'N/A'} and database {args.db or 'N/A'}."
            )
            raise SystemExit(1)
        _print_export(payload, args.market)
    elif args.command == "workflow-export":
        payload = _build_workflow_export_payload(
            workflow_name=args.workflow,
            origin_market=args.origin_market,
            us_results_file=args.us_results_file,
            india_results_file=args.india_results_file,
            db_path=args.db,
            archive_db=args.archive_db,
            out_dir=args.out_dir,
            limit=max(1, args.limit),
            us_venv_dir=args.us_venv_dir,
            india_venv_dir=args.india_venv_dir,
            us_scanner_file=args.us_scanner_file,
            india_scanner_file=args.india_scanner_file,
            us_requirements_file=args.us_requirements_file,
        )
        if payload is None:
            print("Workflow export could not be generated.")
            raise SystemExit(1)
        _print_export(payload, args.origin_market)


if __name__ == "__main__":
    _run_cli()
