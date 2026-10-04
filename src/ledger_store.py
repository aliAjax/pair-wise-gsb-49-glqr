"""记账链 SQLite 存储：事件、分层、赔案、恢复台账、命令去重、链审计。

所有业务写入都在单条 BEGIN IMMEDIATE 事务内完成；命令先以 pending 登记，
写入中断后重启按 pending 重放，重复编号永远返回首次结果。
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

from .domain import Conflict, NotFound


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _m(value: Any) -> float:
    return round(float(value), 2)


class LedgerStore:
    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self.db_path = repository.db_path
        self.init_schema()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        try:
            yield connection
        finally:
            connection.close()

    def init_schema(self) -> None:
        bootstrap = sqlite3.connect(self.db_path, timeout=15)
        try:
            bootstrap.execute("PRAGMA journal_mode = WAL")
            bootstrap.close()
        except sqlite3.Error:
            bootstrap.close()
        with self.transaction() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS cat_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_code TEXT NOT NULL UNIQUE,
                    seq_no INTEGER NOT NULL UNIQUE,
                    name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    payload TEXT NOT NULL DEFAULT '{}',
                    withdrawn_reason TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'live',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS treaty_layers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    layer_code TEXT NOT NULL UNIQUE,
                    contract_ref TEXT NOT NULL DEFAULT '',
                    attachment REAL NOT NULL,
                    limit_amount REAL NOT NULL,
                    cession_pct REAL NOT NULL,
                    reinstatement_count INTEGER NOT NULL,
                    reinstatement_rate REAL NOT NULL,
                    source TEXT NOT NULL DEFAULT 'live',
                    legacy_record_id INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim_number TEXT NOT NULL UNIQUE,
                    event_id INTEGER NOT NULL REFERENCES cat_events(id),
                    layer_id INTEGER NOT NULL REFERENCES treaty_layers(id),
                    loss_amount REAL NOT NULL,
                    reserved_amount REAL NOT NULL DEFAULT 0,
                    confirmed_amount REAL NOT NULL DEFAULT 0,
                    reinstatement_no INTEGER,
                    status TEXT NOT NULL DEFAULT 'assessed',
                    payment_ref TEXT,
                    basis TEXT NOT NULL DEFAULT '{}',
                    source TEXT NOT NULL DEFAULT 'live',
                    legacy_record_id INTEGER,
                    created_by TEXT NOT NULL,
                    assessed_at TEXT NOT NULL,
                    settled_at TEXT,
                    voided_at TEXT
                );
                CREATE TABLE IF NOT EXISTS reinstatement_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim_id INTEGER NOT NULL REFERENCES claims(id),
                    event_id INTEGER NOT NULL REFERENCES cat_events(id),
                    layer_id INTEGER NOT NULL REFERENCES treaty_layers(id),
                    reinstatement_no INTEGER NOT NULL,
                    entry_no INTEGER NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    reinstatement_premium REAL NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'reserved',
                    basis TEXT NOT NULL DEFAULT '{}',
                    source TEXT NOT NULL DEFAULT 'live',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(layer_id, entry_no)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_active_slot
                    ON reinstatement_ledger(layer_id, reinstatement_no)
                    WHERE status IN ('reserved', 'confirmed');
                CREATE UNIQUE INDEX IF NOT EXISTS uq_settlement_payment
                    ON claims(payment_ref) WHERE payment_ref IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_ledger_layer ON reinstatement_ledger(layer_id, status);
                CREATE INDEX IF NOT EXISTS idx_claims_event ON claims(event_id);
                CREATE INDEX IF NOT EXISTS idx_claims_layer ON claims(layer_id);

                CREATE TABLE IF NOT EXISTS idemp_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    command TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    payload TEXT NOT NULL DEFAULT '{}',
                    response TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    source TEXT NOT NULL DEFAULT 'live',
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS chain_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    event_id INTEGER,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'live',
                    details TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_chain_audit_entity ON chain_audit(entity_type, entity_id, id);
                CREATE INDEX IF NOT EXISTS idx_chain_audit_event ON chain_audit(event_id, id);
                CREATE TABLE IF NOT EXISTS recovery_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    report TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS legacy_links (
                    record_id INTEGER PRIMARY KEY,
                    event_id INTEGER NOT NULL,
                    layer_id INTEGER NOT NULL,
                    claim_id INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    # ---------- 基础工具 ----------
    @staticmethod
    def _json(row: Optional[sqlite3.Row], key: str, default: Any) -> Any:
        if row is None:
            return default
        raw = row[key]
        return json.loads(raw) if raw else default

    def audit(self, connection: sqlite3.Connection, entity_type: str, entity_id: Optional[int], action: str,
              actor_id: str, details: Dict[str, Any], source: str = "live", event_id: Optional[int] = None) -> None:
        connection.execute(
            "INSERT INTO chain_audit(entity_type,entity_id,event_id,action,actor_id,source,details,created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (entity_type, entity_id, event_id, action, actor_id, source,
             json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
        )

    def next_event_seq(self, connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT COALESCE(MAX(seq_no), 0) + 1 AS next FROM cat_events").fetchone()
        return int(row["next"])

    # ---------- 命令台账（幂等/续作） ----------
    def stage_command(self, request_id: str, command: str, actor_id: str, payload: Dict[str, Any],
                      source: str = "live") -> None:
        with self.transaction() as connection:
            self.insert_command(connection, request_id, command, actor_id, payload, source)

    def insert_command(self, connection: sqlite3.Connection, request_id: str, command: str, actor_id: str,
                       payload: Dict[str, Any], source: str = "live") -> None:
        try:
            connection.execute(
                "INSERT INTO idemp_commands(request_id,command,actor_id,status,payload,source,created_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (request_id, command, actor_id, "pending",
                 json.dumps(payload, ensure_ascii=False, sort_keys=True), source, _now()),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict("request_id已存在") from exc

    def get_command(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self.read_connection() as connection:
            row = connection.execute("SELECT * FROM idemp_commands WHERE request_id=?", (request_id,)).fetchone()
            return self._command_row(row)

    def find_command_by(self, connection: sqlite3.Connection, column: str, value: str,
                        command: str = None, exclude_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        sql = "SELECT * FROM idemp_commands"
        clauses = []
        params: List[Any] = []
        if column == "payment_ref":
            clauses.append("json_extract(payload, '$.payment_ref') = ?")
            params.append(value)
        elif column == "claim_number":
            clauses.append("json_extract(payload, '$.claim_number') = ?")
            params.append(value)
        elif column == "event_code":
            clauses.append("json_extract(payload, '$.event_code') = ?")
            params.append(value)
        elif column == "layer_code":
            clauses.append("json_extract(payload, '$.layer_code') = ?")
            params.append(value)
        else:
            clauses.append("%s = ?" % column)
            params.append(value)
        if command:
            clauses.append("command = ?")
            params.append(command)
        if exclude_id is not None:
            clauses.append("id <> ?")
            params.append(exclude_id)
        row = connection.execute(sql + " WHERE " + " AND ".join(clauses), params).fetchone()
        return self._command_row(row)

    @staticmethod
    def _command_row(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        item = dict(row)
        item["payload"] = json.loads(item["payload"] or "{}")
        item["response"] = json.loads(item["response"]) if item["response"] else None
        return item

    def complete_command(self, connection: sqlite3.Connection, request_id: str, response: Dict[str, Any]) -> None:
        connection.execute(
            "UPDATE idemp_commands SET status='completed', response=?, error_code=NULL, error_message=NULL,"
            " completed_at=? WHERE request_id=?",
            (json.dumps(response, ensure_ascii=False, sort_keys=True), _now(), request_id),
        )

    def fail_command(self, connection: sqlite3.Connection, request_id: str, code: str, message: str) -> None:
        connection.execute(
            "UPDATE idemp_commands SET status='failed', error_code=?, error_message=?, completed_at=?"
            " WHERE request_id=?",
            (code, message, _now(), request_id),
        )

    def pending_commands(self, connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute("SELECT * FROM idemp_commands WHERE status='pending' ORDER BY id").fetchall()
        return [self._command_row(row) for row in rows]

    def all_pending_commands(self) -> List[Dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute("SELECT * FROM idemp_commands WHERE status='pending' ORDER BY id").fetchall()
            return [self._command_row(row) for row in rows]

    def save_recovery_run(self, kind: str, report: Dict[str, Any]) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO recovery_runs(kind,report,created_at) VALUES(?,?,?)",
                (kind, json.dumps(report, ensure_ascii=False, sort_keys=True), _now()),
            )

    # ---------- 事件 ----------
    def insert_event(self, connection: sqlite3.Connection, event_code: str, seq_no: int, name: str,
                     payload: Dict[str, Any], actor_id: str, source: str = "live") -> Dict[str, Any]:
        now = _now()
        try:
            cursor = connection.execute(
                "INSERT INTO cat_events(event_code,seq_no,name,status,payload,source,created_by,created_at,updated_at)"
                " VALUES(?,?,?, 'active', ?,?,?,?,?)",
                (event_code, seq_no, name, json.dumps(payload, ensure_ascii=False), source, actor_id, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict("事件编号已存在") from exc
        return self.get_event(connection, int(cursor.lastrowid))

    def get_event(self, connection: sqlite3.Connection, event_id: int) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM cat_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFound("巨灾事件不存在")
        return self._event_row(row)

    def find_event(self, connection: sqlite3.Connection, identifier: Any) -> Dict[str, Any]:
        row = self._event_by_identifier(connection, identifier)
        if row is None:
            raise NotFound("巨灾事件不存在")
        return self._event_row(row)

    @staticmethod
    def _event_by_identifier(connection: sqlite3.Connection, identifier: Any) -> Optional[sqlite3.Row]:
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            row = connection.execute("SELECT * FROM cat_events WHERE id=?", (int(identifier),)).fetchone()
            if row:
                return row
        return connection.execute("SELECT * FROM cat_events WHERE event_code=?", (str(identifier),)).fetchone()

    @staticmethod
    def _event_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["payload"] = json.loads(item["payload"] or "{}")
        return item

    def list_events(self) -> List[Dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute("SELECT * FROM cat_events ORDER BY seq_no").fetchall()
            return [self._event_row(row) for row in rows]

    def withdraw_event(self, connection: sqlite3.Connection, event_id: int, reason: str) -> None:
        connection.execute(
            "UPDATE cat_events SET status='withdrawn', withdrawn_reason=?, updated_at=? WHERE id=?",
            (reason, _now(), event_id),
        )

    # ---------- 分层 ----------
    def insert_layer(self, connection: sqlite3.Connection, data: Dict[str, Any], actor_id: str,
                     source: str = "live", legacy_record_id: Optional[int] = None) -> Dict[str, Any]:
        now = _now()
        try:
            cursor = connection.execute(
                "INSERT INTO treaty_layers(layer_code,contract_ref,attachment,limit_amount,cession_pct,"
                "reinstatement_count,reinstatement_rate,source,legacy_record_id,created_by,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (data["layer_code"], data.get("contract_ref", ""), _m(data["attachment"]), _m(data["limit_amount"]),
                 float(data["cession_pct"]), int(data["reinstatement_count"]), _m(data["reinstatement_rate"]),
                 source, legacy_record_id, actor_id, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict("分层编号已存在") from exc
        return self.get_layer(connection, int(cursor.lastrowid))

    def get_layer(self, connection: sqlite3.Connection, layer_id: int) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM treaty_layers WHERE id=?", (layer_id,)).fetchone()
        if row is None:
            raise NotFound("合约分层不存在")
        return dict(row)

    def find_layer(self, connection: sqlite3.Connection, identifier: Any) -> Dict[str, Any]:
        row = None
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            row = connection.execute("SELECT * FROM treaty_layers WHERE id=?", (int(identifier),)).fetchone()
        if row is None:
            row = connection.execute("SELECT * FROM treaty_layers WHERE layer_code=?", (str(identifier),)).fetchone()
        if row is None:
            raise NotFound("合约分层不存在")
        return dict(row)

    def list_layers(self) -> List[Dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute("SELECT * FROM treaty_layers ORDER BY id").fetchall()
            return [dict(row) for row in rows]

    def layer_position(self, connection: sqlite3.Connection, layer_id: int) -> Dict[str, Any]:
        rows = connection.execute(
            "SELECT reinstatement_no, amount, reinstatement_premium, status FROM reinstatement_ledger"
            " WHERE layer_id=? AND status IN ('reserved','confirmed') ORDER BY reinstatement_no",
            (layer_id,),
        ).fetchall()
        slots = [int(row["reinstatement_no"]) for row in rows]
        used = _m(sum(float(row["amount"]) for row in rows))
        reserved_premium = _m(sum(float(row["reinstatement_premium"]) for row in rows if row["status"] == "reserved"))
        confirmed_premium = _m(sum(float(row["reinstatement_premium"]) for row in rows if row["status"] == "confirmed"))
        return {
            "active_slots": slots,
            "used_capacity": used,
            "reserved_premium": reserved_premium,
            "confirmed_premium": confirmed_premium,
            "accumulated_premium": _m(reserved_premium + confirmed_premium),
        }

    def next_ledger_entry_no(self, connection: sqlite3.Connection, layer_id: int) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(entry_no), 0) + 1 AS next FROM reinstatement_ledger WHERE layer_id=?",
            (layer_id,),
        ).fetchone()
        return int(row["next"])

    # ---------- 赔案与台账 ----------
    def insert_claim(self, connection: sqlite3.Connection, data: Dict[str, Any], actor_id: str,
                     source: str = "live", legacy_record_id: Optional[int] = None,
                     status: str = "assessed", now: str = None) -> int:
        now = now or _now()
        assessed_at = now if status in ("assessed", "settled") else None
        settled_at = now if status == "settled" else None
        try:
            cursor = connection.execute(
                "INSERT INTO claims(claim_number,event_id,layer_id,loss_amount,reserved_amount,confirmed_amount,"
                "reinstatement_no,status,payment_ref,basis,source,legacy_record_id,created_by,"
                "assessed_at,settled_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (data["claim_number"], int(data["event_id"]), int(data["layer_id"]), _m(data["loss_amount"]),
                 _m(data.get("reserved_amount", 0)), _m(data.get("confirmed_amount", 0)),
                 data.get("reinstatement_no"), status, data.get("payment_ref"),
                 json.dumps(data.get("basis", {}), ensure_ascii=False, sort_keys=True), source,
                 legacy_record_id, actor_id, assessed_at, settled_at),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict("赔案编号已存在") from exc
        return int(cursor.lastrowid)

    def insert_ledger_entry(self, connection: sqlite3.Connection, entry: Dict[str, Any]) -> int:
        try:
            cursor = connection.execute(
                "INSERT INTO reinstatement_ledger(claim_id,event_id,layer_id,reinstatement_no,entry_no,amount,"
                "reinstatement_premium,status,basis,source,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(entry["claim_id"]), int(entry["event_id"]), int(entry["layer_id"]),
                 int(entry["reinstatement_no"]), int(entry["entry_no"]), _m(entry["amount"]),
                 _m(entry["reinstatement_premium"]), entry["status"],
                 json.dumps(entry.get("basis", {}), ensure_ascii=False, sort_keys=True),
                 entry.get("source", "live"), entry.get("created_at", _now()), entry.get("updated_at", _now())),
            )
        except sqlite3.IntegrityError as exc:
            raise Conflict("恢复次数已被占用") from exc
        return int(cursor.lastrowid)

    def get_claim(self, connection: sqlite3.Connection, claim_id: int) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM claims WHERE id=?", (claim_id,)).fetchone()
        if row is None:
            raise NotFound("赔案不存在")
        return self._claim_row(row)

    def find_claim(self, connection: sqlite3.Connection, identifier: Any) -> Dict[str, Any]:
        row = None
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            row = connection.execute("SELECT * FROM claims WHERE id=?", (int(identifier),)).fetchone()
        if row is None:
            row = connection.execute("SELECT * FROM claims WHERE claim_number=?", (str(identifier),)).fetchone()
        if row is None:
            raise NotFound("赔案不存在")
        return self._claim_row(row)

    @staticmethod
    def _claim_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["basis"] = json.loads(item["basis"] or "{}")
        return item

    def list_claims(self, event_id: Optional[int] = None, layer_id: Optional[int] = None) -> List[Dict[str, Any]]:
        clauses = []
        params: List[Any] = []
        if event_id is not None:
            clauses.append("c.event_id=?")
            params.append(event_id)
        if layer_id is not None:
            clauses.append("c.layer_id=?")
            params.append(layer_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.read_connection() as connection:
            rows = connection.execute(
                "SELECT c.*, e.event_code AS event_code, l.layer_code AS layer_code FROM claims c"
                " JOIN cat_events e ON e.id=c.event_id JOIN treaty_layers l ON l.id=c.layer_id"
                + where + " ORDER BY c.id",
                params,
            ).fetchall()
            return [self._claim_row(row) for row in rows]

    def active_entries_for_event(self, connection: sqlite3.Connection, event_id: int) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM reinstatement_ledger WHERE event_id=? AND status IN ('reserved','confirmed')",
            (event_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["basis"] = json.loads(item["basis"] or "{}")
            result.append(item)
        return result

    def settle_claim(self, connection: sqlite3.Connection, claim_id: int, payment_ref: str,
                     amount: float, premium: float) -> None:
        now = _now()
        connection.execute(
            "UPDATE claims SET status='settled', payment_ref=?, confirmed_amount=?, settled_at=? WHERE id=?",
            (payment_ref, _m(amount), now, claim_id),
        )
        connection.execute(
            "UPDATE reinstatement_ledger SET status='confirmed', amount=?, reinstatement_premium=?, updated_at=?"
            " WHERE claim_id=? AND status='reserved'",
            (_m(amount), _m(premium), now, claim_id),
        )

    def void_event_claims(self, connection: sqlite3.Connection, event_id: int) -> List[int]:
        now = _now()
        rows = connection.execute(
            "SELECT claim_id FROM reinstatement_ledger WHERE event_id=? AND status='reserved'", (event_id,)
        ).fetchall()
        claim_ids = [int(row["claim_id"]) for row in rows]
        connection.execute(
            "UPDATE reinstatement_ledger SET status='released', updated_at=? WHERE event_id=? AND status='reserved'",
            (now, event_id),
        )
        if claim_ids:
            connection.execute(
                "UPDATE claims SET status='void', reserved_amount=0, voided_at=? WHERE id IN (%s)"
                % ",".join("?" for _ in claim_ids),
                [now] + claim_ids,
            )
        return claim_ids

    def ledger_entries_for_claim(self, connection: sqlite3.Connection, claim_id: int) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM reinstatement_ledger WHERE claim_id=? ORDER BY id", (claim_id,)
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["basis"] = json.loads(item["basis"] or "{}")
            result.append(item)
        return result

    # ---------- 链审计 ----------
    def chain_timeline(self, entity_type: str, entity_id: int) -> List[Dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM chain_audit WHERE entity_type=? AND entity_id=? ORDER BY id",
                (entity_type, entity_id),
            ).fetchall()
        return self._audit_rows(rows)

    def event_timeline(self, event_id: int) -> List[Dict[str, Any]]:
        with self.read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM chain_audit WHERE event_id=? ORDER BY id", (event_id,)
            ).fetchall()
        return self._audit_rows(rows)

    @staticmethod
    def _audit_rows(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"] or "{}")
            result.append(item)
        return result

    # ---------- 恢复/对账 ----------
    def has_table(self, table_name: str) -> bool:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
            ).fetchone()
            return row is not None

    def get_meta(self, key: str) -> Optional[str]:
        with self.read_connection() as connection:
            row = connection.execute("SELECT value FROM schema_meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO schema_meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def legacy_record(self, connection: sqlite3.Connection, record_id: int) -> Optional[sqlite3.Row]:
        return connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()

    def list_legacy_records(self, connection: sqlite3.Connection) -> List[sqlite3.Row]:
        return connection.execute("SELECT * FROM records ORDER BY id").fetchall()

    def legacy_link(self, connection: sqlite3.Connection, record_id: int) -> Optional[sqlite3.Row]:
        return connection.execute("SELECT * FROM legacy_links WHERE record_id=?", (record_id,)).fetchone()

    def save_legacy_link(self, connection: sqlite3.Connection, record_id: int, event_id: int,
                         layer_id: int, claim_id: Optional[int], now: str) -> None:
        connection.execute(
            "INSERT INTO legacy_links(record_id,event_id,layer_id,claim_id,created_at) VALUES(?,?,?,?,?)",
            (record_id, event_id, layer_id, claim_id, now),
        )

    def orphan_claims(self, connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT c.* FROM claims c LEFT JOIN reinstatement_ledger r"
            " ON r.claim_id=c.id AND r.status IN ('reserved','confirmed')"
            " WHERE c.status IN ('assessed','settled') AND r.id IS NULL"
        ).fetchall()
        return [self._claim_row(row) for row in rows]

    def mismatched_ledger(self, connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT r.id AS ledger_id, r.status AS ledger_status, c.id AS claim_id, c.status AS claim_status,"
            " r.amount AS amount FROM reinstatement_ledger r JOIN claims c ON c.id=r.claim_id"
            " WHERE r.status IN ('reserved','confirmed')"
            " AND ((r.status='confirmed' AND c.status!='settled')"
            " OR (r.status='reserved' AND c.status NOT IN ('assessed','settled'))"
            " OR (c.status='settled' AND r.status!='confirmed')"
            " OR (c.status='assessed' AND r.status!='reserved'))"
        ).fetchall()
        return [dict(row) for row in rows]

    def sequence_gaps(self, connection: sqlite3.Connection) -> List[Any]:
        rows = connection.execute("SELECT seq_no FROM cat_events ORDER BY seq_no").fetchall()
        seqs = [int(row["seq_no"]) for row in rows]
        gaps = [n for n in range(1, max(seqs, default=0) + 1) if n not in seqs]
        return gaps
