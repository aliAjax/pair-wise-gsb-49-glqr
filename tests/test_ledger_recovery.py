"""写入中断重启：先对账续作；旧数据补事件序列并标来源。"""
import json
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor

CLAIMS = Actor("clm", "claims_officer")
FIN = Actor("fin", "finance")
ADMIN = Actor("adm", "admin")

OLD_DATA = {"event_id": "CAT-OLD", "attachment": 1_000_000.0, "limit": 5_000_000.0,
            "cession_pct": 0.4, "loss_amount": 3_000_000.0, "reinstatement_pct": 0.15,
            "aggregate_prior": 0.0}


class RecoveryBackfillTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "recovery.db")

    def tearDown(self):
        self.temp.cleanup()

    def _build(self, recover):
        return build_service(self.db, recover=recover)

    def test_pending_command_resumed_and_replayed(self):
        service = self._build(False)
        service.ledger.create_event(Actor("a", "cat_analyst"), {"event_code": "CAT-R"}, "ev")
        service.ledger.create_layer(Actor("u", "underwriter"),
                                    {"layer_code": "LR", "attachment": 1_000_000.0,
                                     "limit_amount": 5_000_000.0, "cession_pct": 0.4,
                                     "reinstatement_count": 1, "reinstatement_rate": 0.15}, "ly")
        # 模拟写入中断：命令已 pending，赔案未落库。
        with service.ledger.store.transaction() as connection:
            service.ledger.store.insert_command(
                connection, "rq-resume", "assess", "clm",
                {"claim_number": "CL-R1", "event_code": "CAT-R", "layer_code": "LR",
                 "loss_amount": 3_000_000})

        restarted, report = self._build(True)
        self.assertEqual([item["request_id"] for item in report["resumed"]], ["rq-resume"])
        claim = restarted.ledger.get_claim(FIN, "CL-R1")
        self.assertEqual(claim["status"], "assessed")
        # 同一编号重放返回原结果。
        replay = restarted.ledger.assess(
            CLAIMS, {"claim_number": "CL-R1", "event_code": "CAT-R", "layer_code": "LR",
                     "loss_amount": 3_000_000}, "rq-resume")
        self.assertEqual(replay["id"], claim["id"])
        self.assertEqual(replay["source"], "replay")

    def test_reconcile_rebuilds_missing_and_fixes_mismatch(self):
        service = self._build(False)
        service.ledger.create_event(Actor("a", "cat_analyst"), {"event_code": "CAT-R"}, "ev")
        service.ledger.create_layer(Actor("u", "underwriter"),
                                    {"layer_code": "LR", "attachment": 1_000_000.0,
                                     "limit_amount": 5_000_000.0, "cession_pct": 0.4,
                                     "reinstatement_count": 2, "reinstatement_rate": 0.15}, "ly")
        c1 = service.ledger.assess(CLAIMS, {"claim_number": "CL-1", "event_code": "CAT-R",
                                            "layer_code": "LR", "loss_amount": 2_000_000}, "rq-1")
        c2 = service.ledger.assess(CLAIMS, {"claim_number": "CL-2", "event_code": "CAT-R",
                                            "layer_code": "LR", "loss_amount": 1_500_000}, "rq-2")
        service.ledger.settle(FIN, {"claim_number": "CL-2", "payment_ref": "PAY-2"}, "rq-s2")
        with service.ledger.store.transaction() as connection:
            connection.execute("DELETE FROM reinstatement_ledger WHERE claim_id=?", (c1["id"],))
            connection.execute("UPDATE reinstatement_ledger SET status='reserved' WHERE claim_id=?",
                               (c2["id"],))

        _, report = self._build(True)
        ledger_repairs = {item["claim_id"] for item in report["reconcile"]["repaired_ledger"]}
        self.assertIn(c1["id"], ledger_repairs)
        self.assertTrue(report["reconcile"]["repaired_claims"])
        restarted, second_report = self._build(True)
        self.assertEqual(second_report["reconcile"]["repaired_ledger"], [])
        self.assertEqual(second_report["reconcile"]["repaired_claims"], [])
        fixed = restarted.ledger.get_claim(FIN, "CL-2")
        self.assertEqual(fixed["ledger"][0]["status"], "confirmed")
        self.assertEqual(fixed["confirmed_amount"], 200_000.0)
        rebuilt = restarted.ledger.get_claim(FIN, "CL-1")
        self.assertEqual(rebuilt["ledger"][0]["source"], "recovery")

    def test_legacy_records_backfilled_with_sequence_and_source(self):
        service = self._build(False)
        record = service.create(Actor("cr", "underwriter"), "RI-OLD", OLD_DATA)
        for action, role, data in [
            ("bind", "underwriter", {"underwriter_id": "UW-1"}),
            ("submit_claim", "claims_officer", {"claim_number": "CLM-1", "event_id": "CAT-OLD"}),
            ("calculate", "claims_officer", {"approved_loss": 2_800_000.0}),
            ("settle", "finance", {"payment_reference": "PAY-OLD"}),
        ]:
            record = service.act(Actor("op", role), record["id"], record["version"], action, data)

        _, report = self._build(True)
        self.assertEqual(report["backfill"]["linked"], 1)
        self.assertEqual(report["backfill"]["claims"], ["CLM-1"])

        restarted, second = self._build(True)
        self.assertEqual(second["backfill"]["linked"], 0)
        self.assertEqual(second["backfill"]["skipped"], 1)

        detail = restarted.get_record(Actor("cr", "underwriter"), record["id"])
        self.assertEqual(detail["source"], "legacy")
        self.assertEqual(detail["chain"]["source"], "legacy")
        self.assertEqual(detail["chain"]["event_seq_no"], 1)

        event = restarted.ledger.get_event(FIN, "CAT-OLD")
        self.assertEqual(event["source"], "legacy")
        self.assertEqual(event["seq_no"], 1)
        claim = restarted.ledger.get_claim(FIN, "CLM-1")
        self.assertEqual(claim["status"], "settled")
        self.assertEqual(claim["confirmed_amount"], 720_000.0)
        self.assertEqual(claim["ledger"][0]["source"], "legacy")
        self.assertEqual(claim["basis"]["imported_from_record"], record["id"])

        timeline = restarted.ledger.timeline(FIN, "event", event["id"])
        self.assertTrue(all(item["source"] == "legacy" for item in timeline))
