"""Central configuration loader for LTAP-RP."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class KafkaConfig:
    bootstrap: str
    channel: str
    fetch_max_wait_ms: int = 50


@dataclass
class ArbiterConfig:
    num_agents: int = 4
    tick_interval: float = 0.5
    bid_timeout: float = 8.0
    transmission_timeout: float = 55.0
    cooldown_ticks: int = 1
    max_consecutive_failures: int = 3


@dataclass
class LLMConfig:
    base_url: str
    model: str
    max_tokens: int = 180
    temperature: float = 0.85


@dataclass
class AgentDef:
    name: str
    persona: str
    llm: Optional[LLMConfig] = None  # per-agent override; None means use global default

    def effective_llm(self, default: LLMConfig) -> LLMConfig:
        """Return this agent's LLM config, falling back field-by-field to the global default."""
        if self.llm is None:
            return default
        return LLMConfig(
            base_url=self.llm.base_url or default.base_url,
            model=self.llm.model or default.model,
            max_tokens=self.llm.max_tokens if self.llm.max_tokens is not None else default.max_tokens,
            temperature=self.llm.temperature if self.llm.temperature is not None else default.temperature,
        )


@dataclass
class DemoConfig:
    kafka: KafkaConfig
    arbiter: ArbiterConfig
    llm: LLMConfig
    discussion_topic: str
    agents: list[AgentDef]

    def active_agents(self) -> list[AgentDef]:
        """Return the first num_agents agents (clamped to 2–4)."""
        n = max(2, min(4, self.arbiter.num_agents))
        return self.agents[:n]

    def agent_by_name(self, name: str) -> Optional[AgentDef]:
        return next((a for a in self.agents if a.name == name), None)


def load(path: Optional[str] = None) -> DemoConfig:
    """Load config from *path*, falling back to the CONFIG_PATH env var."""
    resolved = path or os.environ.get("CONFIG_PATH", "/config/config.yaml")
    with open(resolved) as f:
        d = yaml.safe_load(f)

    kd = d["kafka"]
    kafka = KafkaConfig(
        bootstrap=kd["bootstrap"],
        channel=kd["channel"],
        fetch_max_wait_ms=kd.get("fetch_max_wait_ms", 50),
    )

    ad = d.get("arbiter", {})
    arbiter = ArbiterConfig(
        num_agents=ad.get("num_agents", 4),
        tick_interval=ad.get("tick_interval", 0.5),
        bid_timeout=ad.get("bid_timeout", 8.0),
        transmission_timeout=ad.get("transmission_timeout", 55.0),
        cooldown_ticks=ad.get("cooldown_ticks", 1),
        max_consecutive_failures=ad.get("max_consecutive_failures", 3),
    )

    ld = d["llm"]
    llm = LLMConfig(
        base_url=ld["base_url"],
        model=ld["model"],
        max_tokens=ld.get("max_tokens", 180),
        temperature=ld.get("temperature", 0.85),
    )

    agents = []
    for a in d["agents"]:
        agent_llm: Optional[LLMConfig] = None
        if "llm" in a:
            al = a["llm"]
            agent_llm = LLMConfig(
                base_url=al.get("base_url") or llm.base_url,
                model=al.get("model") or llm.model,
                max_tokens=al.get("max_tokens") if al.get("max_tokens") is not None else llm.max_tokens,
                temperature=al.get("temperature") if al.get("temperature") is not None else llm.temperature,
            )
        agents.append(AgentDef(name=a["name"], persona=a["persona"].strip(), llm=agent_llm))

    return DemoConfig(
        kafka=kafka,
        arbiter=arbiter,
        llm=llm,
        discussion_topic=d["discussion"]["topic"].strip(),
        agents=agents,
    )
