"""并发争抢最后恢复次数：只收先到者。"""
import tempfile
import threading
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict


class ConcurrentReinstatementTest(unittest.TestCase):
    def test_only_first_taker_wins(self):
        temp = tempfile.TemporaryDirectory()
        db = str(Path(temp.name) / "concurrent.db")
        service = build_service(db, recover=False)
        ledger = service.ledger
        ledger.create_event(Actor("a", "cat_analyst"), {"event_code": "CAT-C"}, "ev")
        ledger.create_layer(Actor("u", "underwriter"),
                            {"layer_code": "LC", "attachment": 1_000_000.0, "limit_amount": 5_000_000.0,
                             "cession_pct": 0.4, "reinstatement_count": 0,
                             "reinstatement_rate": 0.15}, "ly")

        results = []
        lock = threading.Lock()

        def worker(index):
            local = build_service(db, recover=False)
            try:
                local.ledger.assess(
                    Actor("clm", "claims_officer"),
                    {"claim_number": "CL-%d" % index, "event_code": "CAT-C",
                     "layer_code": "LC", "loss_amount": 3_000_000},
                    "rq-%d" % index)
                with lock:
                    results.append("ok")
            except Conflict:
                with lock:
                    results.append("conflict")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(results.count("ok"), 1)
        self.assertEqual(results.count("conflict"), 11)
        slots = ledger.get_layer(Actor("u", "underwriter"), "LC")["position"]["active_slots"]
        self.assertEqual(slots, [0])
        temp.cleanup()
