"""LLM-backed LTAP participant powered by Ollama."""
import logging
import random
import re

from openai import AsyncOpenAI
from ltap.models import Bid, BidRequest, BusEvent, TransmissionResponse
from ltap.participant import LTAPParticipant

log = logging.getLogger(__name__)

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_THINK_INNER_RE = re.compile(r"<think>(.*?)</think>", re.DOTALL | re.IGNORECASE)


def _strip_think(raw: str) -> str:
    content = _THINK_RE.sub("", raw).strip()
    if content:
        return content
    # Model output was entirely <think>...</think> — extract the last non-empty
    # paragraph from inside the block as a best-effort response.
    m = _THINK_INNER_RE.search(raw)
    if m:
        paragraphs = [p.strip() for p in m.group(1).split("\n\n") if p.strip()]
        return paragraphs[-1] if paragraphs else ""
    return ""
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


class LLMAgent(LTAPParticipant):
    """LTAP participant that calls an Ollama model to generate transmissions.

    Bid priority: 0.92 when the last message was directed at this agent,
    otherwise uniform random in [0.35, 0.75].  Always wants to send.
    """

    def __init__(
        self,
        name: str,
        persona: str,
        topic: str,
        base_url: str,
        model: str,
        max_tokens: int = 180,
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

    def _system_prompt(self) -> str:
        return (
            f"{self._persona}\n\n"
            f"You are in a multi-agent engineering discussion. Topic:\n"
            f"  {self._topic}\n\n"
            "Rules:\n"
            "- Speak in first person as yourself.\n"
            "- Be direct and concise: 2–4 sentences maximum.\n"
            "- Advance the conversation: agree, push back, introduce a concrete example, "
            "raise a failure mode, or propose a specific design. Each turn must move the "
            "discussion forward — do not restate what was already said.\n"
            "- You may address a specific participant by name to hand the conversation to them.\n"
            "- CRITICAL: Never open by echoing, paraphrasing, or restating the previous "
            "message's words or phrases. Your opening sentence must introduce a point that "
            "has not been made yet.\n"
            "- Never open with acknowledgment phrases like '[Name] is right', '[Name] is "
            "correct', 'Exactly', 'That's a good point', 'I think', 'I believe', "
            "'As [name]', or any similar preamble. Your first word must be part of your argument.\n"
            "- Write in plain conversational prose. No markdown, no bold text, no bullet points, "
            "no headers. Do not use the structural pattern 'X creates a Y hazard/failure/problem' "
            "— vary your sentence structure each turn.\n"
            "- Vary your contribution type: sometimes state a concrete position with a specific "
            "number or system name, sometimes push back on what was just said, sometimes propose "
            "a specific implementation detail, sometimes ask the group a pointed question."
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
            )
            raw = resp.choices[0].message.content or ""
            content = _strip_think(raw)
        except Exception:
            log.exception("[%s] Ollama call failed", self.participant_id)
            content = ""

        if not content:
            content = f"[{self.participant_id} has nothing to add right now]"

        return TransmissionResponse(content=content)

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
