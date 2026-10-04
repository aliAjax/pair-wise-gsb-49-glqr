"""并发争抢最后一次恢复：BEGIN IMMEDIATE 事务内复核，只有先到者成功。"""
import tempfile
import threading
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, CapacityExhausted


UW = Actor("uw1", "underwriter")
CO = Actor("co1", "claims_officer")


def layer(total_reinstatements):
    return {
        "layer_code": "L-FLOOD",
        "attachment": 0.0,
        "limit": 1_000_000.0,
        "cession_pct": 1.0,
        "reinstatement_pct": 0.1,
        "reinstatement_total": total_reinstatements,
    }


def claim_data(reference):
    return {
        "event_id": "CAT-FLOOD",
        "layer_code": "L-FLOOD",
        "attachment": 0.0,
        "limit": 1_000_000.0,
        "cession_pct": 1.0,
        "loss_amount": 1_000_000.0,
        "reinstatement_pct": 0.1,
        "reinstatement_total": 1,
        "aggregate_prior": 0.0,
    }


class ConcurrentReserveTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))
        self.service.register_event(UW, {"event_id": "CAT-FLOOD", "event_name": "洪水", "occurred_on": "2026-09-02"})
        # 1层容量+1次恢复：总共只能核定2笔百万摊回
        self.service.register_layer(UW, layer(1))

    def tearDown(self):
        self.temp.cleanup()

    def _prepare_claim(self, reference, barrier, errors, results):
        try:
            record = self.service.create(UW, reference, claim_data(reference))
            record = self.service.act(UW, record["id"], record["version"], "bind", {"underwriter_id": "UW-1"})
            record = self.service.act(CO, record["id"], record["version"], "submit_claim",
                                      {"claim_number": "CLM-" + reference, "event_id": "CAT-FLOOD"})
            barrier.wait()  # 所有线程同时进入核定，争抢最后一次恢复
            calculated = self.service.act(CO, record["id"], record["version"], "calculate",
                                          {"approved_loss": 1_000_000.0})
            results[reference] = calculated
        except CapacityExhausted as exc:
            errors[reference] = str(exc)
        except Exception as exc:  # pragma: no cover - 暴露非预期错误
            errors[reference] = "%s: %s" % (type(exc).__name__, exc)

    def test_parallel_calculate_only_first_gets_last_reinstatement(self):
        # 先占掉首赔（用掉基础容量），再并发争抢唯一的1次恢复
        first = self._prepare_sync("RI-0")
        self.assertEqual(first["payload"]["reserved_amount"], 1_000_000.0)

        barrier = threading.Barrier(4)
        errors, results = {}, {}
        threads = [
            threading.Thread(target=self._prepare_claim,
                             args=("RI-%d" % i, barrier, errors, results))
            for i in range(1, 5)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 仅1笔抢到恢复，其余3笔容量/次数不足
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 3)
        balance = self.service.layer_balance(UW, "L-FLOOD")
        self.assertEqual(balance["reserved_amount"], 2_000_000.0)
        # 首赔吃基础层为0次，第二笔消耗1次恢复
        self.assertEqual(balance["reserved_count"], 1)
        ledger = self.service.ledger(UW, layer_code="L-FLOOD")
        self.assertEqual(len([e for e in ledger if e["entry_type"] == "reserve"]), 2)

    def _prepare_sync(self, reference):
        record = self.service.create(UW, reference, claim_data(reference))
        record = self.service.act(UW, record["id"], record["version"], "bind", {"underwriter_id": "UW-1"})
        record = self.service.act(CO, record["id"], record["version"], "submit_claim",
                                  {"claim_number": "CLM-" + reference, "event_id": "CAT-FLOOD"})
        return self.service.act(CO, record["id"], record["version"], "calculate",
                                {"approved_loss": 1_000_000.0})


if __name__ == "__main__":
    unittest.main()
