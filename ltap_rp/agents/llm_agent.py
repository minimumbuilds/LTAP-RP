"""LLM-backed LTAP participant powered by Ollama."""
import json
import logging
import random
import re

from openai import AsyncOpenAI
from ltap.models import Bid, BidRequest, BusEvent, TransmissionResponse
from ltap.participant import LTAPParticipant

log = logging.getLogger(__name__)

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_THINK_INNER_RE = re.compile(r"<think>(.*?)</think>", re.DOTALL | re.IGNORECASE)

_TX_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "transmission",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string"},
                "addressed_to": {"anyOf": [{"type": "string"}, {"type": "null"}]},
            },
            "required": ["content", "addressed_to"],
            "additionalProperties": False,
        },
    },
}

_MAX_HISTORY = 8

_TURN_DIRECTIVES = [
    "Push back on the most recent point — find a flaw, hidden cost, or edge case in it.",
    "Give a concrete example: cite a real system, algorithm, or specific numbers to back your point.",
    "Propose a specific implementation decision and explain the trade-off it makes.",
    "Raise a failure mode or operational hazard that hasn't come up yet.",
    "Ask the group a pointed question that challenges an assumption being made.",
    "State a position that directly contradicts or qualifies something just said.",
    "Identify the most important unsolved problem in what's been discussed so far.",
    "Argue for the simplest possible approach and explain why complexity isn't justified here.",
]


def _strip_think(raw: str) -> str:
    content = _THINK_RE.sub("", raw).strip()
    if content:
        return content
    m = _THINK_INNER_RE.search(raw)
    if m:
        paragraphs = [p.strip() for p in m.group(1).split("\n\n") if p.strip()]
        return paragraphs[-1] if paragraphs else ""
    return ""


class LLMAgent(LTAPParticipant):
    """LTAP participant that calls an Ollama model to generate transmissions.

    Bid priority: 0.92 when the last message was directed at this agent,
    otherwise uniform random in [0.35, 0.75].  Always wants to send.

    Transmissions use constrained JSON output (response_format=json_schema).
    Models that do not support structured output are not supported.
    """

    def __init__(
        self,
        name: str,
        persona: str,
        topic: str,
        base_url: str,
        model: str,
        participants: list[str],
        max_tokens: int = 480,
        temperature: float = 0.85,
    ) -> None:
        super().__init__(name)
        self._persona = persona
        self._topic = topic
        self._model = model
        self._max_tokens = max_tokens
        self._temperature = temperature
        self._client = AsyncOpenAI(base_url=base_url, api_key="ollama")
        self._history: list[dict] = []
        self._addressed = False
        self._others = [p for p in participants if p != name]

    def _system_prompt(self) -> str:
        others = ", ".join(self._others)
        return (
            f"{self._persona}\n\n"
            f"You are in a multi-agent engineering discussion. Topic:\n"
            f"  {self._topic}\n\n"
            f"Other participants: {others}\n\n"
            "Rules:\n"
            "- Speak in first person as yourself.\n"
            "- Be direct and concise: 2–4 sentences maximum.\n"
            "- Advance the conversation: agree, push back, introduce a concrete example, "
            "raise a failure mode, or propose a specific design. Each turn must move the "
            "discussion forward — do not restate what was already said.\n"
            "- CRITICAL: Never open by echoing, paraphrasing, or restating the previous "
            "message's words or phrases. Your opening sentence must introduce a point that "
            "has not been made yet.\n"
            "- Never open with acknowledgment phrases like '[Name] is right', '[Name] is "
            "correct', 'Exactly', 'That's a good point', 'I think', 'I believe', "
            "'As [name]', or any similar preamble. Your first word must be part of your argument.\n"
            "- Write in plain conversational prose. No markdown, no bold text, no bullet points.\n"
            "- Vary your contribution type each turn.\n\n"
            "Output a JSON object with exactly these fields:\n"
            '  "content": your message as a plain text string\n'
            '  "addressed_to": the name of the participant you are directing your remark to '
            f"(one of: {others}), or null if speaking to the group"
        )

    async def generate_bid(self, request: BidRequest) -> Bid:
        priority = 0.92 if self._addressed else round(0.35 + random.random() * 0.40, 2)
        self._addressed = False
        return Bid(want_to_send=True, priority=priority, intent="speak")

    def _user_turn_prompt(self) -> str:
        has_history = any(m["role"] == "user" for m in self._history)
        if has_history:
            directive = random.choice(_TURN_DIRECTIVES)
            return f"{directive} No preamble — start directly with your argument."
        return "Open the discussion. Start directly with your position, no preamble."

    async def generate_transmission(self, channel_id: str) -> TransmissionResponse:
        log.info("[%s] generating (history_len=%d)", self.participant_id, len(self._history))
        messages = [{"role": "system", "content": self._system_prompt()}]
        messages.extend(self._history[-_MAX_HISTORY:])
        messages.append({"role": "user", "content": self._user_turn_prompt()})

        try:
            resp = await self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=self._max_tokens,
                temperature=self._temperature,
                response_format=_TX_SCHEMA,
            )
            raw = resp.choices[0].message.content or ""
            data = json.loads(_strip_think(raw))
            content = (data.get("content") or "").strip()
            addressed_to = data.get("addressed_to") or None
        except Exception:
            log.exception("[%s] generation failed", self.participant_id)
            return TransmissionResponse(content="")

        return TransmissionResponse(content=content, addressed_to=addressed_to)

    async def on_event(self, event: BusEvent) -> None:
        if event.type != "transmission":
            return
        if event.is_self:
            self._history.append({"role": "assistant", "content": event.content})
        else:
            self._history.append(
                {"role": "user", "content": f"[{event.sender}]: {event.content}"}
            )
        if event.addressed_to == self.participant_id:
            self._addressed = True
