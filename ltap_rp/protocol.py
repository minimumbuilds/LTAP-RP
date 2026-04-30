"""Topic names, routing keys, and JSON helpers for LTAP-RP."""
import json
from typing import Any

# Inbound to arbiter (participants publish here)
TOPIC_BIDS = "ltap.bids"
TOPIC_TRANSMISSIONS = "ltap.transmissions"

# Outbound from arbiter (participants consume here)
TOPIC_BID_REQUESTS = "ltap.bid-requests"
TOPIC_WIN_SIGNALS = "ltap.win-signals"
TOPIC_EVENTS = "ltap.events"

# Control plane (registration acks)
TOPIC_CONTROL = "ltap.control"


def routing_key(channel_id: str, participant_id: str) -> bytes:
    """Key used to target a specific participant on a topic."""
    return f"{channel_id}:{participant_id}".encode()


def correlation_key(prefix: str, channel_id: str, participant_id: str, tick: int) -> str:
    """Key for matching a pending request to its response."""
    return f"{prefix}:{channel_id}:{participant_id}:{tick}"


def encode(data: dict[str, Any]) -> bytes:
    return json.dumps(data).encode()


def decode(data: bytes) -> dict[str, Any]:
    return json.loads(data)
