"""记账链用例测试：预占/确认、幂等重放、撤回失效与依据保留。"""
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, PermissionDenied

CAT_ANALYST = Actor("ana", "cat_analyst")
UW = Actor("uw", "underwriter")
CLAIMS = Actor("clm", "claims_officer")
FIN = Actor("fin", "finance")
ADMIN = Actor("adm", "admin")

LAYER = {"layer_code": "LY-1", "attachment": 1_000_000.0, "limit_amount": 5_000_000.0,
         "cession_pct": 0.4, "reinstatement_count": 1, "reinstatement_rate": 0.15}


class LedgerChainTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "chain.db"), recover=False)
        self.ledger = self.service.ledger
        self.ledger.create_event(CAT_ANALYST, {"event_code": "CAT-1", "name": "台风"}, "rq-event")
        self.ledger.create_layer(UW, LAYER, "rq-layer")

    def tearDown(self):
        self.temp.cleanup()

    def test_assess_reserves_then_settle_confirms(self):
        layer = self.ledger.get_layer(UW, "LY-1")
        self.assertEqual(layer["capacity"], 1_600_000.0)
        self.assertEqual(layer["total_recovery_capacity"], 3_200_000.0)
        claim = self.ledger.assess(
            CLAIMS, {"claim_number": "CL-1", "event_code": "CAT-1", "layer_code": "LY-1",
                     "loss_amount": 3_000_000}, "rq-a1")
        self.assertEqual(claim["status"], "assessed")
        self.assertEqual(claim["reserved_amount"], 800_000.0)
        self.assertEqual(claim["ledger"][0]["status"], "reserved")
        self.assertEqual(claim["ledger"][0]["reinstatement_no"], 0)
        self.assertEqual(claim["ledger"][0]["reinstatement_premium"], 120_000.0)
        position = self.ledger.get_layer(UW, "LY-1")["position"]
        self.assertEqual(position["reserved_premium"], 120_000.0)
        self.assertEqual(position["confirmed_premium"], 0.0)

        settled = self.ledger.settle(FIN, {"claim_number": "CL-1", "payment_ref": "PAY-1"}, "rq-s1")
        self.assertEqual(settled["status"], "settled")
        self.assertEqual(settled["confirmed_amount"], 800_000.0)
        self.assertEqual(settled["ledger"][0]["status"], "confirmed")
        position = self.ledger.get_layer(UW, "LY-1")["position"]
        self.assertEqual(position["reserved_premium"], 0.0)
        self.assertEqual(position["confirmed_premium"], 120_000.0)
        self.assertEqual(position["accumulated_premium"], 120_000.0)

    def test_reinstatements_exhausted(self):
        self.ledger.assess(CLAIMS, {"claim_number": "CL-1", "event_code": "CAT-1",
                                    "layer_code": "LY-1", "loss_amount": 3_000_000}, "rq-a1")
        self.ledger.assess(CLAIMS, {"claim_number": "CL-2", "event_code": "CAT-1",
                                    "layer_code": "LY-1", "loss_amount": 3_000_000}, "rq-a2")
        with self.assertRaises(Conflict):
            self.ledger.assess(CLAIMS, {"claim_number": "CL-3", "event_code": "CAT-1",
                                        "layer_code": "LY-1", "loss_amount": 3_000_000}, "rq-a3")

    def test_capacity_cap_enforced(self):
        # 0 次恢复时总可恢复容量即单层容量，第二单全额挤不进剩余容量。
        self.ledger.create_layer(UW, {**LAYER, "layer_code": "LY-0", "reinstatement_count": 0}, "rq-layer0")
        self.ledger.assess(CLAIMS, {"claim_number": "CL-B1", "event_code": "CAT-1",
                                    "layer_code": "LY-0", "loss_amount": 5_000_000}, "rq-b1")
        with self.assertRaises(Conflict):
            self.ledger.assess(CLAIMS, {"claim_number": "CL-B2", "event_code": "CAT-1",
                                        "layer_code": "LY-0", "loss_amount": 3_000_000}, "rq-b2")

    def test_same_request_id_returns_original_result(self):
        payload = {"claim_number": "CL-1", "event_code": "CAT-1", "layer_code": "LY-1",
                   "loss_amount": 3_000_000}
        first = self.ledger.assess(CLAIMS, payload, "same-key")
        replay = self.ledger.assess(CLAIMS, payload, "same-key")
        self.assertEqual(first["id"], replay["id"])
        self.assertEqual(replay["source"], "replay")
        self.assertTrue(replay["replayed"])
        with self.assertRaises(Conflict):
            self.ledger.assess(CLAIMS, {**payload, "loss_amount": 1.0}, "same-key")
        # 结算重放同样幂等。
        s1 = self.ledger.settle(FIN, {"claim_number": "CL-1", "payment_ref": "PAY-1"}, "settle-key")
        s2 = self.ledger.settle(FIN, {"claim_number": "CL-1", "payment_ref": "PAY-1"}, "settle-key")
        self.assertEqual(s1["id"], s2["id"])
        self.assertEqual(s2["source"], "replay")
        with self.assertRaises(Conflict):
            self.ledger.settle(FIN, {"claim_number": "CL-1", "payment_ref": "PAY-2"}, "settle-key")

    def test_withdraw_voids_unsettled_and_keeps_settled_basis(self):
        c1 = self.ledger.assess(CLAIMS, {"claim_number": "CL-1", "event_code": "CAT-1",
                                         "layer_code": "LY-1", "loss_amount": 3_000_000}, "rq-a1")
        c2 = self.ledger.assess(CLAIMS, {"claim_number": "CL-2", "event_code": "CAT-1",
                                         "layer_code": "LY-1", "loss_amount": 2_000_000}, "rq-a2")
        self.ledger.settle(FIN, {"claim_number": "CL-1", "payment_ref": "PAY-1"}, "rq-s1")
        result = self.ledger.withdraw_event(CAT_ANALYST, "CAT-1", {"reason": "误报"}, "rq-w1")
        self.assertEqual(result["status"], "withdrawn")
        self.assertEqual(result["released_claims"], [c2["id"]])
        self.assertEqual([k["claim_id"] for k in result["kept_settled"]], [c1["id"]])
        # 已结算保留原依据
        kept = self.ledger.get_claim(FIN, "CL-1")
        self.assertEqual(kept["status"], "settled")
        self.assertEqual(kept["payment_ref"], "PAY-1")
        self.assertEqual(kept["basis"]["event_seq_no"], 1)
        self.assertEqual(kept["ledger"][0]["status"], "confirmed")
        # 未结算失效、容量与槽位释放
        voided = self.ledger.get_claim(FIN, "CL-2")
        self.assertEqual(voided["status"], "void")
        self.assertEqual(voided["reserved_amount"], 0.0)
        slots = self.ledger.get_layer(UW, "LY-1")["position"]["active_slots"]
        self.assertEqual(slots, [0])
        # 撤回后不能再核定，撤回本身重放幂等
        with self.assertRaises(Conflict):
            self.ledger.assess(CLAIMS, {"claim_number": "CL-9", "event_code": "CAT-1",
                                        "layer_code": "LY-1", "loss_amount": 1}, "rq-a9")
        again = self.ledger.withdraw_event(CAT_ANALYST, "CAT-1", {"reason": "误报"}, "rq-w1")
        self.assertTrue(again.get("replayed"))

    def test_permissions(self):
        with self.assertRaises(PermissionDenied):
            self.ledger.assess(FIN, {"claim_number": "X", "event_code": "CAT-1",
                                    "layer_code": "LY-1", "loss_amount": 1}, "k")
        with self.assertRaises(PermissionDenied):
            self.ledger.create_layer(CLAIMS, LAYER, "k")
        with self.assertRaises(PermissionDenied):
            self.ledger.reconcile(FIN)

    def test_audit_marks_source(self):
        self.ledger.assess(CLAIMS, {"claim_number": "CL-1", "event_code": "CAT-1",
                                    "layer_code": "LY-1", "loss_amount": 3_000_000}, "rq-a1")
        timeline = self.ledger.timeline(FIN, "event", 1)
        actions = {item["action"]: item for item in timeline}
        self.assertEqual(actions["create_event"]["source"], "live")
        self.assertEqual(actions["assess"]["details"]["source"], "live")
