from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class MainReviewChatPresenceState:
    schema_version: int
    main_target_id: str
    last_seen_at: datetime
