"""SQLite 表结构、迁移回填与事务访问。

记账事实表为 reinstatement_ledger（只追加），layer_balances 是其折叠出的
物化余额。所有容量与恢复次数复核都在 BEGIN IMMEDIATE 事务内完成，保证并发
争抢最后一次恢复时只有先到者成功。
"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .domain import (
    ENTRY_CONFIRM,
    ENTRY_RELEASE,
    ENTRY_RESERVE,
    EVENT_WITHDRAWN,
    SOURCE_LIVE,
    SOURCE_MIGRATION,
    SOURCE_RECONCILE,
    STATE_CALCULATED,
    STATE_INVALIDATED,
    STATE_REJECTED,
    STATE_SETTLED,
    Conflict,
    NotFound,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# 各状态对应的占用金额来源（用于迁移回填）
_RESERVED_STATES = {STATE_CALCULATED}
_CONFIRMED_STATES = {STATE_SETTLED}
_RELEASED_STATES = {STATE_REJECTED}


class Repository:
    def __init__(self, db_path: str, capacity_checker: Callable = None) -> None:
        self.db_path = db_path
        # 事务内容量复核回调：checker(layer, folds, want_amount, want_count)
        self.capacity_checker = capacity_checker
        # 恢复次数规划：planner(layer, folds, recovery) -> 次数
        self.reinstatement_planner = None
        self._init_schema()

    def set_capacity_checker(self, checker: Callable) -> None:
        self.capacity_checker = checker

    def set_reinstatement_planner(self, planner: Callable) -> None:
        # planner(layer, folds, recovery) -> 本笔需要的恢复次数
        self.reinstatement_planner = planner

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    # ------------------------------------------------------------------ schema

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reference TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'live'
                );
                CREATE TABLE IF NOT EXISTS cat_events (
                    event_id TEXT PRIMARY KEY,
                    event_name TEXT NOT NULL,
                    occurred_on TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    detail TEXT NOT NULL DEFAULT '{}',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS layers (
                    layer_code TEXT PRIMARY KEY,
                    attachment REAL NOT NULL,
                    limit_amount REAL NOT NULL,
                    cession_pct REAL NOT NULL,
                    layer_width REAL NOT NULL,
                    layer_capacity REAL NOT NULL,
                    reinstatement_pct REAL NOT NULL,
                    reinstatement_total INTEGER NOT NULL,
                    detail TEXT NOT NULL DEFAULT '{}',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reinstatement_ledger (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_no TEXT NOT NULL UNIQUE,
                    event_id TEXT NOT NULL,
                    layer_code TEXT NOT NULL,
                    claim_id INTEGER REFERENCES records(id) ON DELETE CASCADE,
                    claim_reference TEXT NOT NULL,
                    entry_type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    reinstatement_count INTEGER NOT NULL DEFAULT 0,
                    reinstatement_premium REAL NOT NULL DEFAULT 0,
                    balance_after_amount REAL NOT NULL DEFAULT 0,
                    balance_after_count INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'live',
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS layer_balances (
                    layer_code TEXT PRIMARY KEY REFERENCES layers(layer_code),
                    reserved_amount REAL NOT NULL DEFAULT 0,
                    confirmed_amount REAL NOT NULL DEFAULT 0,
                    released_amount REAL NOT NULL DEFAULT 0,
                    reserved_count INTEGER NOT NULL DEFAULT 0,
                    confirmed_count INTEGER NOT NULL DEFAULT 0,
                    released_count INTEGER NOT NULL DEFAULT 0,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);
                CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_events(record_id, id);
                CREATE INDEX IF NOT EXISTS idx_ledger_layer ON reinstatement_ledger(layer_code, seq);
                CREATE INDEX IF NOT EXISTS idx_ledger_event ON reinstatement_ledger(event_id, seq);
                CREATE INDEX IF NOT EXISTS idx_ledger_claim ON reinstatement_ledger(claim_id, seq);
                """
            )
            # 旧库补列（旧版本 records/audit_events 无事件链字段）
            self._ensure_column(connection, "records", "event_id", "TEXT")
            self._ensure_column(connection, "records", "layer_code", "TEXT")
            self._ensure_column(connection, "audit_events", "source", "TEXT NOT NULL DEFAULT 'live'")
            self._rebuild_audit_if_not_null_record(connection)
            connection.commit()

    @staticmethod
    def _rebuild_audit_if_not_null_record(connection: sqlite3.Connection) -> None:
        """旧库 audit_events.record_id 为 NOT NULL，无法记录事件级审计，需要重建。"""
        cols = connection.execute("PRAGMA table_info(audit_events)").fetchall()
        notnull_record = any(col["name"] == "record_id" and col["notnull"] for col in cols)
        has_source = any(col["name"] == "source" for col in cols)
        if not notnull_record:
            return
        connection.executescript(
            """
            ALTER TABLE audit_events RENAME TO audit_events_legacy;
            CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id INTEGER,
                action TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                details TEXT NOT NULL,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'live'
            );
            """ + (
                "INSERT INTO audit_events(id,record_id,action,actor_id,version,details,created_at,source)"
                " SELECT id,record_id,action,actor_id,version,details,created_at,'live' FROM audit_events_legacy;"
                if has_source else
                "INSERT INTO audit_events(id,record_id,action,actor_id,version,details,created_at,source)"
                " SELECT id,record_id,action,actor_id,version,details,created_at,'live' FROM audit_events_legacy;"
            ) +
            "DROP TABLE audit_events_legacy;"
        )

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, table: str, column: str, decl: str) -> None:
        cols = {row["name"] for row in connection.execute("PRAGMA table_info(%s)" % table).fetchall()}
        if column not in cols:
            connection.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, column, decl))

    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        if "payload" in item and item["payload"]:
            item["payload"] = json.loads(item["payload"])
        if "detail" in item and item["detail"]:
            item["detail"] = json.loads(item["detail"])
        return item

    # ------------------------------------------------------------------ events / layers

    def upsert_event(self, event: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM cat_events WHERE event_id=?", (event["event_id"],)).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO cat_events(event_id,event_name,occurred_on,status,detail,created_by,created_at,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (event["event_id"], event["event_name"], event["occurred_on"], "active",
                     _dumps(event.get("detail", {})), actor_id, now, now),
                )
            connection.commit()
            row = connection.execute("SELECT * FROM cat_events WHERE event_id=?", (event["event_id"],)).fetchone()
        return self._row(row)

    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM cat_events WHERE event_id=?", (event_id,)).fetchone()
        return self._row(row) if row else None

    def require_event_active(self, connection: sqlite3.Connection, event_id: str) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM cat_events WHERE event_id=?", (event_id,)).fetchone()
        if row is None:
            from .domain import ValidationError
            raise ValidationError("巨灾事件不存在，请先注册")
        event = self._row(row)
        if event["status"] == EVENT_WITHDRAWN:
            from .domain import EventInactive
            raise EventInactive("巨灾事件%s已撤回，不能新增或推进赔案" % event_id)
        return event

    def list_events(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM cat_events ORDER BY event_id").fetchall()
        return [self._row(row) for row in rows]

    def upsert_layer(self, layer: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM layers WHERE layer_code=?", (layer["layer_code"],)).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO layers(layer_code,attachment,limit_amount,cession_pct,layer_width,layer_capacity,"
                    "reinstatement_pct,reinstatement_total,detail,created_by,created_at,updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (layer["layer_code"], layer["attachment"], layer["limit"], layer["cession_pct"],
                     layer["layer_width"], layer["layer_capacity"], layer["reinstatement_pct"],
                     int(layer["reinstatement_total"]), _dumps(layer.get("detail", {})), actor_id, now, now),
                )
                connection.execute(
                    "INSERT INTO layer_balances(layer_code,reserved_amount,confirmed_amount,released_amount,"
                    "reserved_count,confirmed_count,released_count,last_seq,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (layer["layer_code"], 0, 0, 0, 0, 0, 0, 0, now),
                )
            connection.commit()
            row = connection.execute("SELECT * FROM layers WHERE layer_code=?", (layer["layer_code"],)).fetchone()
        return self._row(row)

    def get_layer(self, layer_code: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM layers WHERE layer_code=?", (layer_code,)).fetchone()
        return self._row(row) if row else None

    def list_layers(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM layers ORDER BY layer_code").fetchall()
        return [self._row(row) for row in rows]

    def layer_balance(self, layer_code: str) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM layer_balances WHERE layer_code=?", (layer_code,)).fetchone()
        return dict(row) if row else {}

    # ------------------------------------------------------------------ records

    def create(self, reference: str, state: str, payload: Dict[str, Any], actor_id: str,
               event_id: str = None, layer_code: str = None) -> Dict[str, Any]:
        now = _now()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO records(reference,state,version,payload,created_by,updated_by,created_at,updated_at,"
                    "event_id,layer_code) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (reference, state, 1, _dumps(payload), actor_id, actor_id, now, now, event_id, layer_code),
                )
                record_id = int(cursor.lastrowid)
                self._insert_audit(connection, record_id, "created", actor_id, 1,
                                   {"state": state, "source": SOURCE_LIVE}, SOURCE_LIVE, now)
                row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
                connection.commit()
        except sqlite3.IntegrityError as exc:
            raise Conflict("reference已存在") from exc
        return self._row(row)

    def get_by_reference(self, reference: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE reference=?", (reference,)).fetchone()
        return self._row(row) if row else None

    def get(self, record_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        return self._row(row)

    def list_records(self, state: Optional[str] = None, limit: int = 100,
                     event_id: Optional[str] = None) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        clauses, params = [], []
        if state:
            clauses.append("state=?")
            params.append(state)
        if event_id:
            clauses.append("event_id=?")
            params.append(event_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = "SELECT * FROM records" + where + " ORDER BY id DESC LIMIT ?"
        with self._connect() as connection:
            rows = connection.execute(sql, (*params, limit)).fetchall()
        return [self._row(row) for row in rows]

    @staticmethod
    def _insert_audit(connection: sqlite3.Connection, record_id: Optional[int], action: str, actor_id: str,
                      version: int, details: Dict[str, Any], source: str, now: str = None) -> None:
        connection.execute(
            "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at,source)"
            " VALUES(?,?,?,?,?,?,?)",
            (record_id, action, actor_id, version, _dumps(details), now or _now(), source),
        )

    def add_audit(self, record_id: int, actor_id: str, action: str, details: Dict[str, Any],
                  source: str = SOURCE_LIVE) -> None:
        with self._connect() as connection:
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                raise NotFound("记录不存在")
            self._insert_audit(connection, record_id, action, actor_id, int(row["version"]), details, source)
            connection.commit()

    def audit_timeline(self, record_id: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            if record_id is not None:
                connection.execute("SELECT 1 FROM records WHERE id=?", (record_id,)).fetchone()
                rows = connection.execute(
                    "SELECT * FROM audit_events WHERE record_id=? ORDER BY id LIMIT ?", (record_id, limit)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def stats(self) -> Dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT state, COUNT(*) AS total FROM records GROUP BY state").fetchall()
        return {str(row["state"]): int(row["total"]) for row in rows}

    def health(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # ------------------------------------------------------------------ ledger

    @staticmethod
    def _fold_entries(rows: List[sqlite3.Row]) -> Dict[str, Any]:
        reserved_amount = confirmed_amount = released_amount = 0.0
        reserved_count = confirmed_count = released_count = 0
        for row in rows:
            kind, amount = row["entry_type"], float(row["amount"])
            count = int(row["reinstatement_count"])
            if kind == ENTRY_RESERVE:
                reserved_amount += amount
                reserved_count += count
            elif kind == ENTRY_CONFIRM:
                confirmed_amount += amount
                confirmed_count += count
            elif kind == ENTRY_RELEASE:
                released_amount += amount
                released_count += count
        return {
            "reserved_amount": round(reserved_amount, 2),
            "confirmed_amount": round(confirmed_amount, 2),
            "released_amount": round(released_amount, 2),
            "outstanding_amount": round(reserved_amount - confirmed_amount - released_amount, 2),
            "reserved_count": reserved_count,
            "confirmed_count": confirmed_count,
            "released_count": released_count,
            "outstanding_count": reserved_count - confirmed_count - released_count,
        }

    def list_ledger(self, layer_code: str = None, event_id: str = None, claim_id: int = None,
                    limit: int = 200) -> List[Dict[str, Any]]:
        clauses, params = [], []
        if layer_code:
            clauses.append("layer_code=?")
            params.append(layer_code)
        if event_id:
            clauses.append("event_id=?")
            params.append(event_id)
        if claim_id is not None:
            clauses.append("claim_id=?")
            params.append(claim_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM reinstatement_ledger" + where + " ORDER BY seq DESC LIMIT ?", (*params, limit)
            ).fetchall()
        return [dict(row) for row in rows]

    def _insert_ledger(self, connection: sqlite3.Connection, *, entry_no: str, event_id: str, layer_code: str,
                       claim_id: int, claim_reference: str, entry_type: str, amount: float,
                       reinstatement_count: int, reinstatement_premium: float, reason: str,
                       source: str, actor_id: str, now: str) -> Dict[str, Any]:
        # 余额快照随本次记账同步推进
        bal_row = connection.execute("SELECT * FROM layer_balances WHERE layer_code=?", (layer_code,)).fetchone()
        bal = dict(bal_row) if bal_row else None
        if bal is None:
            raise NotFound("合约分层%s不存在" % layer_code)
        amount = round(float(amount), 2)
        count = int(reinstatement_count)
        if entry_type == ENTRY_RESERVE:
            bal["reserved_amount"] = round(bal["reserved_amount"] + amount, 2)
            bal["reserved_count"] += count
        elif entry_type == ENTRY_CONFIRM:
            bal["confirmed_amount"] = round(bal["confirmed_amount"] + amount, 2)
            bal["confirmed_count"] += count
        elif entry_type == ENTRY_RELEASE:
            bal["released_amount"] = round(bal["released_amount"] + amount, 2)
            bal["released_count"] += count
        outstanding_amount = round(bal["reserved_amount"] - bal["confirmed_amount"] - bal["released_amount"], 2)
        outstanding_count = bal["reserved_count"] - bal["confirmed_count"] - bal["released_count"]
        cursor = connection.execute(
            "INSERT INTO reinstatement_ledger(entry_no,event_id,layer_code,claim_id,claim_reference,entry_type,"
            "amount,reinstatement_count,reinstatement_premium,balance_after_amount,balance_after_count,reason,"
            "source,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (entry_no, event_id, layer_code, claim_id, claim_reference, entry_type, amount, count,
             round(float(reinstatement_premium), 2), outstanding_amount, outstanding_count,
             reason, source, actor_id, now),
        )
        seq = int(cursor.lastrowid)
        connection.execute(
            "UPDATE layer_balances SET reserved_amount=?,confirmed_amount=?,released_amount=?,reserved_count=?,"
            "confirmed_count=?,released_count=?,last_seq=?,updated_at=? WHERE layer_code=?",
            (bal["reserved_amount"], bal["confirmed_amount"], bal["released_amount"], bal["reserved_count"],
             bal["confirmed_count"], bal["released_count"], seq, now, layer_code),
        )
        row = connection.execute("SELECT * FROM reinstatement_ledger WHERE seq=?", (seq,)).fetchone()
        return dict(row)

    def _existing_entry(self, connection: sqlite3.Connection, entry_no: str) -> Optional[Dict[str, Any]]:
        row = connection.execute("SELECT * FROM reinstatement_ledger WHERE entry_no=?", (entry_no,)).fetchone()
        return dict(row) if row else None

    def _claim_ledger(self, connection: sqlite3.Connection, claim_id: int) -> List[sqlite3.Row]:
        return connection.execute(
            "SELECT * FROM reinstatement_ledger WHERE claim_id=? ORDER BY seq", (claim_id,)
        ).fetchall()

    # ---------- 核定：预占容量与恢复次数（事务内复核，先到者得） ----------

    def reserve_capacity(self, *, claim_id: int, expected_version: int, approved_loss: float,
                         recovery: float, reinstatement_premium: float,
                         actor_id: str, source: str = SOURCE_LIVE) -> Dict[str, Any]:
        now = _now()
        entry_no = "RSV-%s" % claim_id
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._existing_entry(connection, entry_no)
            if existing is not None:
                # 同编号重放：返回原结果，不再占用
                connection.commit()
                return {"replayed": True, "entry": existing, "record": self.get(claim_id)}
            row = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if row["state"] == STATE_INVALIDATED:
                connection.rollback()
                from .domain import EventInactive
                raise EventInactive("赔案因巨灾事件撤回已失效，不能继续操作")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            record = self._row(row)
            event_id, layer_code, reference = row["event_id"], row["layer_code"], row["reference"]
            self.require_event_active(connection, event_id)
            layer = self._row(connection.execute("SELECT * FROM layers WHERE layer_code=?", (layer_code,)).fetchone())
            folds = self._fold_entries(connection.execute(
                "SELECT * FROM reinstatement_ledger WHERE layer_code=?", (layer_code,)).fetchall())
            # 次数在锁内按实时占用计算：首赔吃基础层为0次，避免并发下的TOCTOU
            want_count = int(self.reinstatement_planner(layer, folds, recovery)) \
                if self.reinstatement_planner else (1 if recovery > 0 else 0)
            # 并发争抢最后一次恢复：锁内复核，超限者拒绝
            if self.capacity_checker is not None:
                self.capacity_checker(layer, folds, recovery, want_count)
            payload = dict(record["payload"])
            payload["approved_loss"] = round(float(approved_loss), 2)
            payload["recoverable_amount"] = round(float(recovery), 2)
            payload["reinstatement_premium"] = round(float(reinstatement_premium), 2)
            payload["reserved_amount"] = round(float(recovery), 2)
            payload["reserved_reinstatements"] = int(want_count)
            new_state = STATE_CALCULATED
            new_version = int(row["version"]) + 1
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (new_state, new_version, _dumps(payload), actor_id, now, claim_id),
            )
            entry = self._insert_ledger(
                connection, entry_no=entry_no, event_id=event_id, layer_code=layer_code, claim_id=claim_id,
                claim_reference=reference, entry_type=ENTRY_RESERVE, amount=recovery,
                reinstatement_count=want_count, reinstatement_premium=0.0,
                reason="核定预占层容量与恢复次数", source=source, actor_id=actor_id, now=now,
            )
            self._insert_audit(connection, claim_id, "calculate", actor_id, new_version, {
                "summary": "摊回金额已核定并预占层容量",
                "from": record["state"], "to": new_state,
                "approved_loss": payload["approved_loss"], "recoverable_amount": payload["recoverable_amount"],
                "reinstatement_count": want_count,
                "ledger_seq": entry["seq"], "entry_no": entry_no, "source": source,
            }, source, now)
            result = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            connection.commit()
        return {"replayed": False, "entry": entry, "record": self._row(result)}

    # ---------- 结算：预占转实耗，累计恢复保费 ----------

    def confirm_consumption(self, *, claim_id: int, expected_version: int, payment_reference: str,
                            actor_id: str, source: str = SOURCE_LIVE) -> Dict[str, Any]:
        now = _now()
        entry_no = "CNF-%s" % claim_id
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._existing_entry(connection, entry_no)
            if existing is not None:
                connection.commit()
                return {"replayed": True, "entry": existing, "record": self.get(claim_id)}
            row = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if row["state"] == STATE_INVALIDATED:
                connection.rollback()
                from .domain import EventInactive
                raise EventInactive("赔案因巨灾事件撤回已失效，不能继续操作")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            record = self._row(row)
            reserve = self._existing_entry(connection, "RSV-%s" % claim_id)
            if reserve is None or record["state"] != STATE_CALCULATED:
                connection.rollback()
                raise Conflict("赔案未完成核定预占，不能结算")
            amount = float(reserve["amount"])
            want_count = int(reserve["reinstatement_count"])
            if amount <= 0:
                connection.rollback()
                from .domain import ValidationError
                raise ValidationError("无可结算摊回")
            layer = self._row(connection.execute(
                "SELECT * FROM layers WHERE layer_code=?", (row["layer_code"],)).fetchone())
            premium = round(amount * float(layer["reinstatement_pct"]), 2)
            payload = dict(record["payload"])
            payload["payment_reference"] = payment_reference
            payload["confirmed_amount"] = amount
            payload["confirmed_reinstatements"] = want_count
            payload["reinstatement_premium_confirmed"] = premium
            new_state = STATE_SETTLED
            new_version = int(row["version"]) + 1
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (new_state, new_version, _dumps(payload), actor_id, now, claim_id),
            )
            entry = self._insert_ledger(
                connection, entry_no=entry_no, event_id=row["event_id"], layer_code=row["layer_code"],
                claim_id=claim_id, claim_reference=row["reference"], entry_type=ENTRY_CONFIRM,
                amount=amount, reinstatement_count=want_count, reinstatement_premium=premium,
                reason="结算确认实耗并累计恢复保费", source=source, actor_id=actor_id, now=now,
            )
            self._insert_audit(connection, claim_id, "settle", actor_id, new_version, {
                "summary": "预占转为实耗，恢复保费已累计",
                "from": record["state"], "to": new_state,
                "payment_reference": payment_reference, "amount": amount,
                "reinstatement_premium": premium,
                "ledger_seq": entry["seq"], "entry_no": entry_no, "source": source,
            }, source, now)
            result = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            connection.commit()
        return {"replayed": False, "entry": entry, "record": self._row(result)}

    # ---------- 拒赔：释放预占 ----------

    def reject_claim(self, *, claim_id: int, expected_version: int, reject_reason: str,
                     actor_id: str, source: str = SOURCE_LIVE) -> Dict[str, Any]:
        now = _now()
        entry_no = "REL-%s" % claim_id
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._existing_entry(connection, entry_no)
            if existing is not None:
                connection.commit()
                return {"replayed": True, "entry": existing, "record": self.get(claim_id)}
            row = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            record = self._row(row)
            if record["state"] not in {"claim_submitted", STATE_CALCULATED}:
                connection.rollback()
                raise Conflict("当前状态不允许执行reject")
            reserve = self._existing_entry(connection, "RSV-%s" % claim_id)
            payload = dict(record["payload"])
            payload["reject_reason"] = reject_reason
            new_state = STATE_REJECTED
            new_version = int(row["version"]) + 1
            entry = None
            if reserve is not None and self._existing_entry(connection, "CNF-%s" % claim_id) is None:
                # 已核定未结算：释放预占
                payload["reserved_amount"] = 0.0
                payload["reserved_reinstatements"] = 0
                entry = self._insert_ledger(
                    connection, entry_no=entry_no, event_id=row["event_id"], layer_code=row["layer_code"],
                    claim_id=claim_id, claim_reference=row["reference"], entry_type=ENTRY_RELEASE,
                    amount=float(reserve["amount"]), reinstatement_count=int(reserve["reinstatement_count"]),
                    reinstatement_premium=0.0, reason="赔案拒绝，释放预占：%s" % reject_reason,
                    source=source, actor_id=actor_id, now=now,
                )
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (new_state, new_version, _dumps(payload), actor_id, now, claim_id),
            )
            self._insert_audit(connection, claim_id, "reject", actor_id, new_version, {
                "summary": "赔案已拒绝" + ("，预占释放" if entry else ""),
                "from": record["state"], "to": new_state, "reject_reason": reject_reason,
                "ledger_seq": entry["seq"] if entry else None, "source": source,
            }, source, now)
            result = connection.execute("SELECT * FROM records WHERE id=?", (claim_id,)).fetchone()
            connection.commit()
        return {"replayed": False, "entry": entry, "record": self._row(result)}

    # ---------- 简单状态动作（bind/submit_claim），仍走乐观锁 ----------

    def mutate(self, record_id: int, expected_version: int, state: str, payload: Dict[str, Any], actor_id: str,
               action: str, details: Dict[str, Any], source: str = SOURCE_LIVE) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            record = self._row(row)
            if action == "submit_claim":
                self.require_event_active(connection, row["event_id"])
            version = int(expected_version) + 1
            details = dict(details)
            details["source"] = source
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (state, version, _dumps(payload), actor_id, now, record_id),
            )
            self._insert_audit(connection, record_id, action, actor_id, version, details, source, now)
            result = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            connection.commit()
        return self._row(result)

    # ---------- 事件撤回：未结算失效并释放，已结算保留依据 ----------

    def withdraw_event(self, event_id: str, actor_id: str, reason: str,
                       source: str = SOURCE_LIVE) -> Dict[str, Any]:
        now = _now()
        released_claims, preserved_claims = [], []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            event = self.require_event_active(connection, event_id)
            rows = connection.execute(
                "SELECT * FROM records WHERE event_id=? ORDER BY id", (event_id,)).fetchall()
            for row in rows:
                claim_id = int(row["id"])
                reserve = self._existing_entry(connection, "RSV-%s" % claim_id)
                confirm = self._existing_entry(connection, "CNF-%s" % claim_id)
                if reserve is not None and confirm is None:
                    # 未结算：释放预占，赔案失效
                    release_no = "REL-%s" % claim_id
                    entry = self._existing_entry(connection, release_no)
                    if entry is None:
                        entry = self._insert_ledger(
                            connection, entry_no=release_no, event_id=event_id, layer_code=row["layer_code"],
                            claim_id=claim_id, claim_reference=row["reference"], entry_type=ENTRY_RELEASE,
                            amount=float(reserve["amount"]),
                            reinstatement_count=int(reserve["reinstatement_count"]),
                            reinstatement_premium=0.0,
                            reason="事件撤回，未结算预占失效：%s" % reason,
                            source=source, actor_id=actor_id, now=now,
                        )
                    record = self._row(row)
                    payload = dict(record["payload"])
                    payload["reserved_amount"] = 0.0
                    payload["reserved_reinstatements"] = 0
                    new_version = int(row["version"]) + 1
                    connection.execute(
                        "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                        (STATE_INVALIDATED, new_version, _dumps(payload), actor_id, now, claim_id),
                    )
                    self._insert_audit(connection, claim_id, "event_withdrawn", actor_id, new_version, {
                        "summary": "巨灾事件撤回，未结算记录失效，预占已释放",
                        "event_id": event_id, "reason": reason,
                        "ledger_seq": entry["seq"], "source": source,
                    }, source, now)
                    released_claims.append({"claim_id": claim_id, "reference": row["reference"],
                                            "ledger_seq": entry["seq"]})
                elif confirm is not None:
                    # 已结算：原依据（confirm 台账）保留不动
                    preserved_claims.append({"claim_id": claim_id, "reference": row["reference"],
                                             "confirmed_seq": confirm["seq"]})
            connection.execute(
                "UPDATE cat_events SET status=?,updated_at=? WHERE event_id=?", (EVENT_WITHDRAWN, now, event_id)
            )
            self._insert_audit(connection, None, "event_withdrawn", actor_id, 0, {
                "summary": "巨灾事件撤回", "event_id": event_id, "reason": reason,
                "released_claims": released_claims, "preserved_claims": preserved_claims, "source": source,
            }, source, now)
            connection.commit()
        return {"event_id": event_id, "released": released_claims, "preserved": preserved_claims}

    # ------------------------------------------------------------------ migration

    def needs_legacy_backfill(self) -> bool:
        """旧库：records 有数据但台账为空，需要补事件序列。"""
        with self._connect() as connection:
            total = connection.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"]
            ledger = connection.execute("SELECT COUNT(*) AS c FROM reinstatement_ledger").fetchone()["c"]
        return total > 0 and ledger == 0

    def backfill_legacy(self) -> Dict[str, int]:
        """旧数据补事件序列：为旧赔案补建事件/分层，按状态补记台账。

        每一条台账 entry_no 固定（RSV-/CNF-/REL-/id），重复执行不产生重复账。
        """
        now = _now()
        summary = {"claims": 0, "events": 0, "layers": 0, "reserve": 0, "confirm": 0, "release": 0}
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute("SELECT * FROM records ORDER BY id").fetchall()
            for row in rows:
                record = self._row(row)
                p = record["payload"]
                event_id = row["event_id"] or p.get("event_id")
                layer_code = row["layer_code"] or ("L-%s" % row["id"])
                recovery = round(float(p.get("recoverable_amount", 0) or 0), 2)
                premium = round(float(p.get("reinstatement_premium", 0) or 0), 2)
                if not event_id:
                    continue
                if connection.execute("SELECT 1 FROM cat_events WHERE event_id=?", (event_id,)).fetchone() is None:
                    connection.execute(
                        "INSERT INTO cat_events(event_id,event_name,occurred_on,status,detail,created_by,"
                        "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        (event_id, "旧数据事件%s" % event_id, "", "active",
                         _dumps({"backfilled": True}), "system", now, now),
                    )
                    summary["events"] += 1
                if connection.execute("SELECT 1 FROM layers WHERE layer_code=?", (layer_code,)).fetchone() is None:
                    width = round(float(p.get("layer_width", float(p["limit"]) - float(p["attachment"]))), 2)
                    cession = float(p["cession_pct"])
                    connection.execute(
                        "INSERT INTO layers(layer_code,attachment,limit_amount,cession_pct,layer_width,"
                        "layer_capacity,reinstatement_pct,reinstatement_total,detail,created_by,created_at,"
                        "updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (layer_code, float(p["attachment"]), float(p["limit"]), cession, width,
                         round(width * cession, 2), float(p.get("reinstatement_pct", 0)), 1,
                         _dumps({"backfilled": True, "from_claim": row["reference"]}),
                         "system", now, now),
                    )
                    connection.execute(
                        "INSERT INTO layer_balances(layer_code,reserved_amount,confirmed_amount,released_amount,"
                        "reserved_count,confirmed_count,released_count,last_seq,updated_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?)",
                        (layer_code, 0, 0, 0, 0, 0, 0, 0, now),
                    )
                    summary["layers"] += 1
                connection.execute(
                    "UPDATE records SET event_id=?,layer_code=? WHERE id=?", (event_id, layer_code, row["id"])
                )
                claim_id = int(row["id"])
                state = row["state"]
                # 旧数据按记账顺序回填，次数依据当时的实时占用重放（首赔吃基础层为0次）
                folds = self._fold_entries(connection.execute(
                    "SELECT * FROM reinstatement_ledger WHERE layer_code=? ORDER BY seq",
                    (layer_code,)).fetchall())
                base_capacity = round(width * cession, 2)
                used = folds["confirmed_amount"] + folds["outstanding_amount"]
                if recovery > 0 and used + recovery > base_capacity + 0.01:
                    count_one = max(1, int(round((used + recovery) / base_capacity)) - 1)
                else:
                    count_one = 0
                if state in _RESERVED_STATES and recovery > 0:
                    self._backfill_entry(connection, "RSV-%s" % claim_id, event_id, layer_code, row,
                                         ENTRY_RESERVE, recovery, count_one, 0.0,
                                         "旧数据补事件序列：核定预占", now)
                    summary["reserve"] += 1
                elif state in _CONFIRMED_STATES and recovery > 0:
                    self._backfill_entry(connection, "RSV-%s" % claim_id, event_id, layer_code, row,
                                         ENTRY_RESERVE, recovery, count_one, 0.0,
                                         "旧数据补事件序列：核定预占", now)
                    self._backfill_entry(connection, "CNF-%s" % claim_id, event_id, layer_code, row,
                                         ENTRY_CONFIRM, recovery, count_one, premium,
                                         "旧数据补事件序列：结算确认", now)
                    summary["reserve"] += 1
                    summary["confirm"] += 1
                elif state in _RELEASED_STATES and recovery > 0:
                    self._backfill_entry(connection, "RSV-%s" % claim_id, event_id, layer_code, row,
                                         ENTRY_RESERVE, recovery, count_one, 0.0,
                                         "旧数据补事件序列：核定预占", now)
                    self._backfill_entry(connection, "REL-%s" % claim_id, event_id, layer_code, row,
                                         ENTRY_RELEASE, recovery, count_one, 0.0,
                                         "旧数据补事件序列：拒绝释放", now)
                    summary["reserve"] += 1
                    summary["release"] += 1
                summary["claims"] += 1
            self._insert_audit(connection, None, "legacy_backfill", "system", 0, {
                "summary": "旧数据补事件序列完成", **summary, "source": SOURCE_MIGRATION,
            }, SOURCE_MIGRATION, now)
            connection.commit()
        return summary

    def _backfill_entry(self, connection: sqlite3.Connection, entry_no: str, event_id: str, layer_code: str,
                        row: sqlite3.Row, entry_type: str, amount: float, count: int, premium: float,
                        reason: str, now: str) -> None:
        if self._existing_entry(connection, entry_no) is not None:
            return
        self._insert_ledger(
            connection, entry_no=entry_no, event_id=event_id, layer_code=layer_code,
            claim_id=int(row["id"]), claim_reference=row["reference"], entry_type=entry_type,
            amount=amount, reinstatement_count=count, reinstatement_premium=premium,
            reason=reason, source=SOURCE_MIGRATION, actor_id="system", now=now,
        )

    # ------------------------------------------------------------------ reconcile

    def reconcile(self, actor_id: str = "system") -> Dict[str, Any]:
        """重启对账续作：以台账为唯一事实，重建层余额并修补半成品。

        - 层余额与台账折叠不一致：以台账重算；
        - calculated/rejected 缺台账：补账（模拟写入中断后续作）；
        - 台账显示已释放但赔案未失效：纠正赔案状态；
        - 撤回事件下仍有未结算预占：释放并失效。
        """
        now = _now()
        report = {"balances_rebuilt": [], "missing_reserve": 0, "state_fixed": 0, "withdrawn_released": 0}
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # 1) 先修补赔案状态与台账（可能补记账目）
            rows = connection.execute("SELECT * FROM records ORDER BY id").fetchall()
            for row in rows:
                claim_id, state = int(row["id"]), row["state"]
                event_id, layer_code, reference = row["event_id"], row["layer_code"], row["reference"]
                reserve = self._existing_entry(connection, "RSV-%s" % claim_id)
                confirm = self._existing_entry(connection, "CNF-%s" % claim_id)
                release = self._existing_entry(connection, "REL-%s" % claim_id)
                record = self._row(row)
                payload = dict(record["payload"])
                fixed = False
                # 1a) 已核定但无预占账（中断在核定与记账之间）：补预占
                if state == STATE_CALCULATED and reserve is None:
                    recovery = round(float(payload.get("recoverable_amount", 0) or 0), 2)
                    if recovery > 0 and layer_code:
                        layer_row = connection.execute(
                            "SELECT * FROM layers WHERE layer_code=?", (layer_code,)).fetchone()
                        layer_info = self._row(layer_row)
                        folds_now = self._fold_entries(connection.execute(
                            "SELECT * FROM reinstatement_ledger WHERE layer_code=? ORDER BY seq",
                            (layer_code,)).fetchall())
                        need_count = int(self.reinstatement_planner(layer_info, folds_now, recovery)) \
                            if self.reinstatement_planner else int(payload.get("reserved_reinstatements", 0) or 0)
                        self._insert_ledger(
                            connection, entry_no="RSV-%s" % claim_id, event_id=event_id, layer_code=layer_code,
                            claim_id=claim_id, claim_reference=reference, entry_type=ENTRY_RESERVE,
                            amount=recovery, reinstatement_count=need_count,
                            reinstatement_premium=0.0, reason="对账续作：补记核定预占",
                            source=SOURCE_RECONCILE, actor_id=actor_id, now=now,
                        )
                        payload.setdefault("reserved_amount", recovery)
                        payload.setdefault("reserved_reinstatements", need_count)
                        report["missing_reserve"] += 1
                        fixed = True
                # 1b) 已有确认账但赔案未到 settled（中断在 confirm 之后）：状态补齐
                if confirm is not None and state != STATE_SETTLED:
                    payload.setdefault("confirmed_amount", round(float(confirm["amount"]), 2))
                    payload.setdefault("confirmed_reinstatements", int(confirm["reinstatement_count"]))
                    payload.setdefault("reinstatement_premium_confirmed",
                                       round(float(confirm["reinstatement_premium"]), 2))
                    connection.execute(
                        "UPDATE records SET state=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                        (STATE_SETTLED, _dumps(payload), actor_id, now, claim_id),
                    )
                    self._insert_audit(connection, claim_id, "reconcile_fix", actor_id, int(row["version"]), {
                        "summary": "对账续作：结算台账已存在，补齐赔案结算状态",
                        "ledger_seq": confirm["seq"], "source": SOURCE_RECONCILE,
                    }, SOURCE_RECONCILE, now)
                    report["state_fixed"] += 1
                    continue
                # 1c) 已有释放账但赔案未 rejected/invalidated：纠正状态并释放字段
                if release is not None and state not in {STATE_REJECTED, STATE_INVALIDATED}:
                    payload["reserved_amount"] = 0.0
                    payload["reserved_reinstatements"] = 0
                    connection.execute(
                        "UPDATE records SET state=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                        (STATE_INVALIDATED, _dumps(payload), actor_id, now, claim_id),
                    )
                    self._insert_audit(connection, claim_id, "reconcile_fix", actor_id, int(row["version"]), {
                        "summary": "对账续作：释放台账已存在，赔案置为失效",
                        "ledger_seq": release["seq"], "source": SOURCE_RECONCILE,
                    }, SOURCE_RECONCILE, now)
                    report["state_fixed"] += 1
                elif fixed:
                    connection.execute(
                        "UPDATE records SET payload=?,updated_by=?,updated_at=? WHERE id=?",
                        (_dumps(payload), actor_id, now, claim_id),
                    )
            # 2) 撤回事件下仍占用的未结算预占：释放并失效
            withdrawn = connection.execute(
                "SELECT event_id FROM cat_events WHERE status=?", (EVENT_WITHDRAWN,)).fetchall()
            for ev in withdrawn:
                event_id = ev["event_id"]
                for row in connection.execute(
                        "SELECT * FROM records WHERE event_id=? ORDER BY id", (event_id,)).fetchall():
                    claim_id = int(row["id"])
                    reserve = self._existing_entry(connection, "RSV-%s" % claim_id)
                    confirm = self._existing_entry(connection, "CNF-%s" % claim_id)
                    if reserve is not None and confirm is None and \
                            self._existing_entry(connection, "REL-%s" % claim_id) is None:
                        entry = self._insert_ledger(
                            connection, entry_no="REL-%s" % claim_id, event_id=event_id,
                            layer_code=row["layer_code"], claim_id=claim_id, claim_reference=row["reference"],
                            entry_type=ENTRY_RELEASE, amount=float(reserve["amount"]),
                            reinstatement_count=int(reserve["reinstatement_count"]),
                            reinstatement_premium=0.0, reason="对账续作：撤回事件未结算预占释放",
                            source=SOURCE_RECONCILE, actor_id=actor_id, now=now,
                        )
                        record = self._row(row)
                        payload = dict(record["payload"])
                        payload["reserved_amount"] = 0.0
                        payload["reserved_reinstatements"] = 0
                        connection.execute(
                            "UPDATE records SET state=?,version=version+1,payload=?,updated_by=?,updated_at=? WHERE id=?",
                            (STATE_INVALIDATED, _dumps(payload), actor_id, now, claim_id),
                        )
                        self._insert_audit(connection, claim_id, "reconcile_fix", actor_id,
                                           int(row["version"]) + 1, {
                                "summary": "对账续作：撤回事件下未结算预占释放并失效",
                                "ledger_seq": entry["seq"], "source": SOURCE_RECONCILE,
                            }, SOURCE_RECONCILE, now)
                        report["withdrawn_released"] += 1
            # 3) 所有账目定稿后，逐层以台账为唯一事实重建余额
            layers = connection.execute("SELECT layer_code FROM layers ORDER BY layer_code").fetchall()
            for layer_row in layers:
                code = layer_row["layer_code"]
                entries = connection.execute(
                    "SELECT * FROM reinstatement_ledger WHERE layer_code=? ORDER BY seq", (code,)).fetchall()
                folds = self._fold_entries(entries)
                last_seq = int(entries[-1]["seq"]) if entries else 0
                bal_row = connection.execute(
                    "SELECT * FROM layer_balances WHERE layer_code=?", (code,)).fetchone()
                current = dict(bal_row) if bal_row else None
                expected = (folds["reserved_amount"], folds["confirmed_amount"], folds["released_amount"],
                            folds["reserved_count"], folds["confirmed_count"], folds["released_count"], last_seq)
                actual = None if current is None else (
                    round(float(current["reserved_amount"]), 2), round(float(current["confirmed_amount"]), 2),
                    round(float(current["released_amount"]), 2), int(current["reserved_count"]),
                    int(current["confirmed_count"]), int(current["released_count"]), int(current["last_seq"]))
                if actual != expected:
                    connection.execute(
                        "UPDATE layer_balances SET reserved_amount=?,confirmed_amount=?,released_amount=?,"
                        "reserved_count=?,confirmed_count=?,released_count=?,last_seq=?,updated_at=?"
                        " WHERE layer_code=?",
                        (*expected, now, code),
                    )
                    report["balances_rebuilt"].append({"layer_code": code, "last_seq": last_seq})
            self._insert_audit(connection, None, "reconcile", actor_id, 0, {
                "summary": "启动对账完成", **report, "source": SOURCE_RECONCILE,
            }, SOURCE_RECONCILE, now)
            connection.commit()
        return report

    def premium_summary(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(SUM(reinstatement_premium),0) AS premium,"
                " COALESCE(SUM(CASE WHEN entry_type='confirm' THEN amount ELSE 0 END),0) AS confirmed"
                " FROM reinstatement_ledger").fetchone()
        return {"confirmed_reinstatement_premium": round(float(row["premium"]), 2),
                "confirmed_recovery": round(float(row["confirmed"]), 2)}
