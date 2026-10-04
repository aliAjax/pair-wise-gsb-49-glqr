"""审计事件封装，时间线查询保持只读。"""
from typing import Any, Dict, List, Optional


class AuditRecorder:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def timeline(self, record_id: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
        return self.repository.audit_timeline(record_id, limit=limit)

    def note(self, record_id: Optional[int], actor_id: str, action: str, details: Dict[str, Any]) -> None:
        self.repository.add_audit(record_id, actor_id, action, details)
