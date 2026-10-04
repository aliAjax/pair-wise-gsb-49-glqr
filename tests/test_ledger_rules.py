"""记账链规则的纯函数测试。"""
import unittest

from src.ledger_rules import LedgerRules, choose_slot, recoverable, reinstatement_premium, layer_capacity


class LedgerRulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = LedgerRules()
        self.layer = {"attachment": 1_000_000.0, "limit_amount": 5_000_000.0,
                      "cession_pct": 0.4, "reinstatement_count": 2, "reinstatement_rate": 0.15}

    def test_capacity_and_recovery(self):
        self.assertEqual(layer_capacity(1_000_000, 5_000_000, 0.4), 1_600_000.0)
        self.assertEqual(recoverable(3_000_000, self.layer), 800_000.0)
        self.assertEqual(recoverable(6_000_000, self.layer), 1_600_000.0)
        self.assertEqual(recoverable(500_000, self.layer), 0.0)
        self.assertEqual(reinstatement_premium(800_000, self.layer), 120_000.0)

    def test_slot_choice(self):
        self.assertEqual(choose_slot(set(), 2), 0)
        self.assertEqual(choose_slot({0}, 2), 1)
        self.assertEqual(choose_slot({0, 1}, 2), 2)
        self.assertIsNone(choose_slot({0, 1, 2}, 2))

    def test_validation(self):
        data = self.rules.validate_assess({"claim_number": "C", "event_code": "E", "layer_id": 3,
                                           "loss_amount": 10})
        self.assertEqual(data["layer_id"], 3)
        with self.assertRaises(Exception):
            self.rules.validate_assess({"claim_number": "C", "loss_amount": 10})
        with self.assertRaises(Exception):
            self.rules.validate_layer({"layer_code": "L", "attachment": 100, "limit_amount": 90,
                                       "cession_pct": 0.4, "reinstatement_count": 1,
                                       "reinstatement_rate": 0.1})
