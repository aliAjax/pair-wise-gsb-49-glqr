import unittest

from src.domain import Actor, ValidationError
from src.rules import DomainRules


CREATE_DATA = {'event_id': 'CAT-2026-01', 'attachment': 1000000.0, 'limit': 5000000.0, 'cession_pct': 0.4, 'loss_amount': 3000000.0, 'reinstatement_pct': 0.15, 'aggregate_prior': 0.0}
FLOW = [('bind', 'underwriter', {'underwriter_id': 'UW-8'}, 'bound'), ('submit_claim', 'claims_officer', {'claim_number': 'CLM-88', 'event_id': 'CAT-2026-01'}, 'claim_submitted'), ('calculate', 'claims_officer', {'approved_loss': 2800000.0}, 'calculated'), ('settle', 'finance', {'payment_reference': 'PAY-1'}, 'settled')]


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = DomainRules()

    def test_prepare_create(self):
        prepared = self.rules.prepare_create(CREATE_DATA)
        self.assertEqual(prepared["layer_width"], 4000000.0)
        self.assertEqual(prepared["recoverable_amount"], 800000.0)
        self.assertEqual(prepared["reinstatement_premium"], 120000.0)

    def test_action_calculation(self):
        action, role, data, expected_state = FLOW[0]
        record = {"id": 1, "state": self.rules.INITIAL_STATE, "payload": self.rules.prepare_create(CREATE_DATA)}
        state, payload, summary = self.rules.apply_action(record, action, data)
        self.assertEqual(state, expected_state)
        self.assertEqual(payload["bound_by"], "UW-8")

    def test_invalid_input(self):
        invalid = dict(CREATE_DATA)
        invalid["cession_pct"] = 1.5
        with self.assertRaises(ValidationError):
            self.rules.prepare_create(invalid)
