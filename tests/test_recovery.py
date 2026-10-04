"""写入中断后重启：旧数据补事件序列，对账续作修补半成品。"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import (
    Actor,
    SOURCE_LIVE,
    SOURCE_MIGRATION,
    SOURCE_RECONCILE,
    STATE_CALCULATED,
    STATE_SETTLED,
)


UW = Actor("uw1", "underwriter")
CO = Actor("co1", "claims_officer")
FIN = Actor("fin1", "finance")


def old_payload(recoverable, premium):
    return {
        "event_id": "CAT-OLD",
        "attachment": 1_000_000.0,
        "limit": 3_000_000.0,
        "cession_pct": 0.5,
        "loss_amount": 3_000_000.0,
        "reinstatement_pct": 0.2,
        "aggregate_prior": 0.0,
        "layer_width": 2_000_000.0,
        "recoverable_amount": recoverable,
        "reinstatement_premium": premium,
        "net_retention": 2_000_000.0,
    }


class StartupRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp.name) / "test.db")

    def tearDown(self):
        self.temp.cleanup()

    def _create_old_database(self):
        # 模拟旧版本库：只有 records/audit_events 两张表
        connection = sqlite3.connect(self.db_path)
        connection.executescript(
            """
            CREATE TABLE records (
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
            CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                details TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        rows = [
            ("OLD-1", STATE_CALCULATED, old_payload(500_000.0, 100_000.0)),
            ("OLD-2", STATE_SETTLED, old_payload(500_000.0, 100_000.0)),
        ]
        for reference, state, payload in rows:
            connection.execute(
                "INSERT INTO records(reference,state,version,payload,created_by,updated_by,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (reference, state, 4, json.dumps(payload), "legacy", "legacy", "2026-08-01T00:00:00+00:00",
                 "2026-08-01T00:00:00+00:00"),
            )
        connection.commit()
        connection.close()

    def test_legacy_database_backfill_event_sequence(self):
        self._create_old_database()
        service = build_service(self.db_path, recover=True)

        events = service.list_events(UW)
        self.assertEqual([e["event_id"] for e in events], ["CAT-OLD"])
        layers = service.list_layers(UW)
        self.assertEqual(len(layers), 2)

        # 未结算旧赔案：补 reserve；已结算旧赔案：补 reserve+confirm 且累计保费
        unsettled = service.repository.get_by_reference("OLD-1")
        settled = service.repository.get_by_reference("OLD-2")
        ledger_u = sorted(service.ledger(UW, claim_id=unsettled["id"]), key=lambda e: e["seq"])
        ledger_s = sorted(service.ledger(CO, claim_id=settled["id"]), key=lambda e: e["seq"])
        self.assertEqual([e["entry_type"] for e in ledger_u], ["reserve"])
        self.assertEqual([e["entry_type"] for e in ledger_s], ["reserve", "confirm"])
        self.assertTrue(all(e["source"] == SOURCE_MIGRATION for e in ledger_u + ledger_s))
        self.assertEqual(service.stats(FIN)["premium"]["confirmed_reinstatement_premium"], 100_000.0)

        # 详情标出来源：旧台账 migration + 旧审计（source 默认 live）
        detail = service.get_record(CO, settled["id"])
        self.assertEqual(detail["ledger_sources"], [SOURCE_MIGRATION])

        # 再重启：补账幂等，不重复
        again = build_service(self.db_path, recover=True)
        self.assertEqual(again.repository.reconcile(actor_id="system")["balances_rebuilt"], [])
        ledger_rows = again.repository.list_ledger(limit=500)
        self.assertEqual(len(ledger_rows), 3)

    def test_interrupted_write_is_repaired_on_restart(self):
        # 正常办理一笔核定，制造层余额与台账不一致（模拟写入中断）
        service = build_service(self.db_path)
        service.repository.upsert_event(
            {"event_id": "CAT-X", "event_name": "风暴", "occurred_on": "2026-09-03"}, "uw1")
        service.repository.upsert_layer({
            "layer_code": "L-X", "attachment": 0.0, "limit": 1_000_000.0, "cession_pct": 1.0,
            "layer_width": 1_000_000.0, "layer_capacity": 1_000_000.0,
            "reinstatement_pct": 0.1, "reinstatement_total": 2, "detail": {},
        }, "uw1")
        record = service.create(UW, "RI-X", {
            "event_id": "CAT-X", "layer_code": "L-X", "attachment": 0.0, "limit": 1_000_000.0,
            "cession_pct": 1.0, "loss_amount": 800_000.0, "reinstatement_pct": 0.1,
            "reinstatement_total": 2, "aggregate_prior": 0.0,
        })
        record = service.act(UW, record["id"], record["version"], "bind", {"underwriter_id": "UW-1"})
        record = service.act(CO, record["id"], record["version"], "submit_claim",
                             {"claim_number": "CLM-X", "event_id": "CAT-X"})
        record = service.act(CO, record["id"], record["version"], "calculate",
                             {"approved_loss": 800_000.0})
        # 中断：余额表被破坏/清零，台账仍是事实
        connection = sqlite3.connect(self.db_path)
        connection.execute("UPDATE layer_balances SET reserved_amount=0,reserved_count=0,last_seq=0")
        connection.commit()
        connection.close()

        # 重启对账：余额按台账重建（首赔吃基础层，恢复次数占用为0）
        restarted = build_service(self.db_path, recover=True)
        balance = restarted.layer_balance(UW, "L-X")
        self.assertEqual(balance["reserved_amount"], 800_000.0)
        self.assertEqual(balance["reserved_count"], 0)
        self.assertEqual(balance["last_seq"], 1)

        # 审计标出 reconcile 来源
        timeline = restarted.timeline(UW, limit=50)
        reconcile_events = [e for e in timeline if e["action"] == "reconcile"]
        self.assertTrue(reconcile_events)
        self.assertTrue(all(e["source"] == SOURCE_RECONCILE for e in reconcile_events))

    def test_interrupted_settle_completed_from_ledger(self):
        # confirm 台账已写但赔案状态停在 calculated（结算中途崩溃）
        service = build_service(self.db_path)
        service.repository.upsert_event(
            {"event_id": "CAT-Y", "event_name": "冰雹", "occurred_on": "2026-09-04"}, "uw1")
        service.repository.upsert_layer({
            "layer_code": "L-Y", "attachment": 0.0, "limit": 1_000_000.0, "cession_pct": 1.0,
            "layer_width": 1_000_000.0, "layer_capacity": 1_000_000.0,
            "reinstatement_pct": 0.1, "reinstatement_total": 1, "detail": {},
        }, "uw1")
        record = service.create(UW, "RI-Y", {
            "event_id": "CAT-Y", "layer_code": "L-Y", "attachment": 0.0, "limit": 1_000_000.0,
            "cession_pct": 1.0, "loss_amount": 600_000.0, "reinstatement_pct": 0.1,
            "reinstatement_total": 1, "aggregate_prior": 0.0,
        })
        record = service.act(UW, record["id"], record["version"], "bind", {"underwriter_id": "UW-1"})
        record = service.act(CO, record["id"], record["version"], "submit_claim",
                             {"claim_number": "CLM-Y", "event_id": "CAT-Y"})
        calculated = service.act(CO, record["id"], record["version"], "calculate",
                                 {"approved_loss": 600_000.0})
        # 正常结算后把状态回滚到 calculated（模拟只写了台账没提交状态）
        settled = service.act(FIN, calculated["id"], calculated["version"], "settle",
                              {"payment_reference": "PAY-Y"})
        connection = sqlite3.connect(self.db_path)
        connection.execute("UPDATE records SET state=? WHERE id=?", (STATE_CALCULATED, settled["id"]))
        connection.commit()
        connection.close()

        restarted = build_service(self.db_path, recover=True)
        fixed = restarted.get_record(CO, settled["id"])
        self.assertEqual(fixed["state"], STATE_SETTLED)
        self.assertEqual(fixed["payload"]["confirmed_amount"], 600_000.0)
        fix_events = [e for e in restarted.timeline(CO, settled["id"]) if e["action"] == "reconcile_fix"]
        self.assertTrue(fix_events)
        self.assertEqual(fix_events[0]["source"], SOURCE_RECONCILE)
