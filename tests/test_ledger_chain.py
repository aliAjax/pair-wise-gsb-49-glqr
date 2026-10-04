"""记账链：核定预占、结算确认、同号重放、事件撤回、来源标记。"""
import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import (
    Actor,
    CapacityExhausted,
    EventInactive,
    SOURCE_LIVE,
    SOURCE_REPLAY,
    STATE_CALCULATED,
    STATE_INVALIDATED,
    STATE_SETTLED,
)


UW = Actor("uw1", "underwriter")
CO = Actor("co1", "claims_officer")
FIN = Actor("fin1", "finance")

LAYER = {
    "layer_code": "L-QUAKE-1",
    "attachment": 1_000_000.0,
    "limit": 2_000_000.0,            # 层宽 100万
    "cession_pct": 1.0,
    "reinstatement_pct": 0.1,
    "reinstatement_total": 1,       # 仅1次恢复：总容量200万
}
EVENT = {"event_id": "CAT-QUAKE", "event_name": "示例地震", "occurred_on": "2026-09-01"}


def claim_data(reference, loss):
    return {
        "event_id": "CAT-QUAKE",
        "layer_code": "L-QUAKE-1",
        "attachment": 1_000_000.0,
        "limit": 2_000_000.0,
        "cession_pct": 1.0,
        "loss_amount": loss,
        "reinstatement_pct": 0.1,
        "reinstatement_total": 1,
        "aggregate_prior": 0.0,
    }


class LedgerChainTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.service.register_event(UW, EVENT)
        self.service.register_layer(UW, LAYER)

    def tearDown(self):
        self.temp.cleanup()

    def _create_submit_calculate(self, reference, loss, approved_loss=None):
        record = self.service.create(UW, reference, claim_data(reference, loss))
        record = self.service.act(UW, record["id"], record["version"], "bind", {"underwriter_id": "UW-1"})
        record = self.service.act(CO, record["id"], record["version"], "submit_claim",
                                  {"claim_number": "CLM-" + reference, "event_id": "CAT-QUAKE"})
        record = self.service.act(CO, record["id"], record["version"], "calculate",
                                  {"approved_loss": approved_loss if approved_loss is not None else loss})
        return record

    def test_reserve_blocks_capacity_before_settlement(self):
        # 核定摊回80万：预占容量与1次恢复，但保费未确认
        record = self._create_submit_calculate("RI-1", 1_800_000.0)
        self.assertEqual(record["state"], STATE_CALCULATED)
        self.assertEqual(record["payload"]["reserved_amount"], 800_000.0)
        # 首赔吃基础层容量，不消耗恢复次数
        self.assertEqual(record["payload"]["reserved_reinstatements"], 0)
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["reserved_amount"], 800_000.0)
        self.assertEqual(balance["confirmed_amount"], 0.0)
        self.assertEqual(balance["reserved_count"], 0)
        # 结算前恢复保费不计入累计
        premium = self.service.stats(UW)["premium"]
        self.assertEqual(premium["confirmed_reinstatement_premium"], 0.0)

        settled = self.service.act(FIN, record["id"], record["version"], "settle",
                                   {"payment_reference": "PAY-1"})
        self.assertEqual(settled["state"], STATE_SETTLED)
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["confirmed_amount"], 800_000.0)
        # 预占口径在结算后仍保留（reserved 累计），未结清为零
        self.assertEqual(balance["outstanding_amount"], 0.0)
        premium = self.service.stats(UW)["premium"]
        self.assertEqual(premium["confirmed_reinstatement_premium"], 80_000.0)

    def test_calculate_replay_returns_original_reservation(self):
        record = self._create_submit_calculate("RI-2", 1_500_000.0)
        version = record["version"]
        # 同编号（同赔案同核定）重放：即便版本号陈旧也返回原结果，不重复占用
        replay = self.service.act(CO, record["id"], version - 1, "calculate",
                                  {"approved_loss": 1_900_000.0})
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["source"], SOURCE_REPLAY)
        # 金额仍是第一次核定的50万
        self.assertEqual(replay["payload"]["recoverable_amount"], 500_000.0)
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["reserved_amount"], 500_000.0)
        self.assertEqual(balance["reserved_count"], 0)

    def test_settle_replay_is_idempotent(self):
        record = self._create_submit_calculate("RI-3", 1_500_000.0)
        settled = self.service.act(FIN, record["id"], record["version"], "settle",
                                   {"payment_reference": "PAY-3"})
        replay = self.service.act(FIN, settled["id"], settled["version"], "settle",
                                  {"payment_reference": "PAY-3"})
        self.assertTrue(replay["replayed"])
        ledger = self.service.ledger(UW, claim_id=settled["id"])
        confirms = [e for e in ledger if e["entry_type"] == "confirm"]
        self.assertEqual(len(confirms), 1)
        self.assertEqual(self.service.stats(UW)["premium"]["confirmed_reinstatement_premium"], 50_000.0)

    def test_last_reinstatement_only_first_wins_concurrent_claimants(self):
        # 基础容量100万+1次恢复=总200万。第一笔核定100万吃满基础层（0次恢复）
        self._create_submit_calculate("RI-A", 2_000_000.0)
        # 第二笔核定100万用掉唯一1次恢复
        self._create_submit_calculate("RI-B", 2_000_000.0)
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["reserved_count"], 1)
        # 第三笔：恢复次数已耗尽
        with self.assertRaises(CapacityExhausted):
            self._create_submit_calculate("RI-C", 1_100_000.0)
        self.assertEqual(balance["reserved_amount"], 2_000_000.0)

    def test_reject_releases_reservation(self):
        record = self._create_submit_calculate("RI-4", 1_800_000.0)
        rejected = self.service.act(FIN, record["id"], record["version"], "reject",
                                    {"reject_reason": "材料不符"})
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["released_amount"], 800_000.0)
        self.assertEqual(balance["outstanding_amount"], 0.0)
        self.assertEqual(balance["outstanding_count"], 0)
        ledger = self.service.ledger(UW, claim_id=rejected["id"])
        types = sorted(e["entry_type"] for e in ledger)
        self.assertEqual(types, ["release", "reserve"])
        # 释放后容量可再次使用
        again = self._create_submit_calculate("RI-5", 1_800_000.0)
        self.assertEqual(again["state"], STATE_CALCULATED)

    def test_withdraw_invalidates_unsettled_preserves_settled(self):
        unsettled = self._create_submit_calculate("RI-U", 1_500_000.0)
        settled = self._create_submit_calculate("RI-S", 1_300_000.0)
        settled = self.service.act(FIN, settled["id"], settled["version"], "settle",
                                   {"payment_reference": "PAY-S"})
        result = self.service.withdraw_event(UW, "CAT-QUAKE", "事件核定撤销")
        released_refs = {item["reference"] for item in result["released"]}
        preserved_refs = {item["reference"] for item in result["preserved"]}
        self.assertIn("RI-U", released_refs)
        self.assertIn("RI-S", preserved_refs)

        detail_u = self.service.get_record(CO, unsettled["id"])
        self.assertEqual(detail_u["state"], STATE_INVALIDATED)
        detail_s = self.service.get_record(CO, settled["id"])
        self.assertEqual(detail_s["state"], STATE_SETTLED)
        # 已结算依据（confirm台账）保留
        self.assertTrue(any(e["entry_type"] == "confirm" for e in detail_s["ledger"]))
        # 未结算赔案不能继续操作
        with self.assertRaises(EventInactive):
            self.service.act(FIN, unsettled["id"], unsettled["version"], "settle",
                             {"payment_reference": "PAY-X"})
        # 撤回事件下不能新建赔案
        with self.assertRaises(EventInactive):
            self._create_submit_calculate("RI-N", 1_200_000.0)
        # 预占已释放
        balance = self.service.layer_balance(UW, "L-QUAKE-1")
        self.assertEqual(balance["outstanding_amount"], 0.0)

    def test_detail_and_audit_mark_sources(self):
        record = self._create_submit_calculate("RI-6", 1_500_000.0)
        detail = self.service.get_record(CO, record["id"])
        self.assertEqual(detail["event"]["event_id"], "CAT-QUAKE")
        self.assertEqual(detail["layer"]["layer_code"], "L-QUAKE-1")
        self.assertEqual({e["entry_type"] for e in detail["ledger"]}, {"reserve"})
        self.assertIn(SOURCE_LIVE, detail["audit_sources"])
        timeline = self.service.timeline(CO, record["id"])
        calc_event = [e for e in timeline if e["action"] == "calculate"][0]
        self.assertEqual(calc_event["source"], SOURCE_LIVE)
        self.assertIn("ledger_seq", calc_event["details"])
