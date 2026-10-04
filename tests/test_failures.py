import tempfile
import unittest
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, PermissionDenied


CREATE_DATA = {'event_id': 'CAT-2026-01', 'attachment': 1000000.0, 'limit': 5000000.0, 'cession_pct': 0.4, 'loss_amount': 3000000.0, 'reinstatement_pct': 0.15, 'aggregate_prior': 0.0}
FLOW = [('bind', 'underwriter', {'underwriter_id': 'UW-8'}, 'bound'), ('submit_claim', 'claims_officer', {'claim_number': 'CLM-88', 'event_id': 'CAT-2026-01'}, 'claim_submitted'), ('calculate', 'claims_officer', {'approved_loss': 2800000.0}, 'calculated'), ('settle', 'finance', {'payment_reference': 'PAY-1'}, 'settled')]


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))

    def tearDown(self):
        self.temp.cleanup()

    def test_permission_and_duplicate(self):
        with self.assertRaises(PermissionDenied):
            self.service.create(Actor("outsider", "outsider"), "RI-25001", CREATE_DATA)
        self.service.create(Actor("creator", "underwriter"), "RI-25001", CREATE_DATA)
        with self.assertRaises(Conflict):
            self.service.create(Actor("creator", "underwriter"), "RI-25001", CREATE_DATA)

    def test_stale_version_is_rejected(self):
        record = self.service.create(Actor("creator", "underwriter"), "RI-25001", CREATE_DATA)
        first = FLOW[0]
        record = self.service.act(Actor("operator", first[1]), record["id"], record["version"], first[0], first[2])
        second = FLOW[1]
        with self.assertRaises(Conflict):
            self.service.act(Actor("operator", second[1]), record["id"], record["version"] - 1, second[0], second[2])
