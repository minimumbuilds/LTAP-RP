"""Agent process: looks up its definition in config and runs an LLMAgent via Kafka."""
import asyncio
import logging
import os
import sys

from .agents.llm_agent import LLMAgent
from .config import load as load_config
from .runner import KafkaParticipantRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


async def main() -> None:
    name = os.environ["AGENT_NAME"]
    cfg = load_config(os.environ.get("CONFIG_PATH"))

    agent_def = cfg.agent_by_name(name)
    if agent_def is None:
        log.error("Agent '%s' not found in config. Available: %s", name, [a.name for a in cfg.agents])
        sys.exit(1)

    k = cfg.kafka
    llm = agent_def.effective_llm(cfg.llm)
    participants = [a.name for a in cfg.active_agents()]

    agent = LLMAgent(
        name=name,
        persona=agent_def.persona,
        topic=cfg.discussion_topic,
        base_url=llm.base_url,
        model=llm.model,
        participants=participants,
        max_tokens=llm.max_tokens,
        temperature=llm.temperature,
    )
    runner = KafkaParticipantRunner(
        agent,
        k.channel,
        k.bootstrap,
        fetch_max_wait_ms=k.fetch_max_wait_ms,
    )

    log.info(
        "Agent '%s' starting (channel=%s, model=%s, base_url=%s)",
        name, k.channel, llm.model, llm.base_url,
    )
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
