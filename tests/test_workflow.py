import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict


CREATE_DATA = {'event_id': 'CAT-2026-01', 'attachment': 1000000.0, 'limit': 5000000.0, 'cession_pct': 0.4, 'loss_amount': 3000000.0, 'reinstatement_pct': 0.15, 'aggregate_prior': 0.0}
FLOW = [('bind', 'underwriter', {'underwriter_id': 'UW-8'}, 'bound'), ('submit_claim', 'claims_officer', {'claim_number': 'CLM-88', 'event_id': 'CAT-2026-01'}, 'claim_submitted'), ('calculate', 'claims_officer', {'approved_loss': 2800000.0}, 'calculated'), ('settle', 'finance', {'payment_reference': 'PAY-1'}, 'settled')]


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))

    def tearDown(self):
        self.temp.cleanup()

    def test_complete_workflow_and_audit(self):
        record = self.service.create(Actor("creator", "underwriter"), "RI-25001", CREATE_DATA)
        self.assertEqual(record["state"], "quoted")
        for action, role, data, expected_state in FLOW:
            record = self.service.act(Actor("operator", role), record["id"], record["version"], action, data)
            self.assertEqual(record["state"], expected_state)
        timeline = self.service.timeline(Actor("creator", "underwriter"), record["id"])
        self.assertEqual(len(timeline), len(FLOW) + 1)
        self.assertEqual(timeline[-1]["action"], FLOW[-1][0])
