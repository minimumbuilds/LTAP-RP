"""Arbiter process: registers participants from config and runs the LTAP tick loop."""
import asyncio
import logging
import os

from ltap.arbiter import Arbiter

from .config import load as load_config
from .connector import KafkaConnectorManager, KafkaParticipantConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


async def main() -> None:
    cfg = load_config(os.environ.get("CONFIG_PATH"))
    k, a = cfg.kafka, cfg.arbiter
    active = cfg.active_agents()

    manager = KafkaConnectorManager(k.bootstrap, fetch_max_wait_ms=k.fetch_max_wait_ms)
    await manager.start()
    log.info("Kafka manager started (bootstrap=%s)", k.bootstrap)

    arbiter = Arbiter(
        cooldown_ticks=a.cooldown_ticks,
        bid_timeout=a.bid_timeout,
        transmission_timeout=a.transmission_timeout,
        max_consecutive_failures=a.max_consecutive_failures,
        tick_interval=a.tick_interval,
    )
    arbiter.create_channel(k.channel)
    log.info(
        "Channel '%s' created (tick_interval=%.2fs, num_agents=%d)",
        k.channel, a.tick_interval, len(active),
    )

    for agent_def in active:
        connector = KafkaParticipantConnector(agent_def.name, k.channel, manager)
        member_list = await arbiter.register_participant(k.channel, agent_def.name, connector)
        log.info("Registered '%s'; members: %s", agent_def.name, member_list.participants)
        await manager.publish_control(
            k.channel,
            agent_def.name,
            {
                "type": "registered",
                "channel_id": k.channel,
                "tick": member_list.tick,
                "participants": member_list.participants,
            },
        )

    log.info("All %d participants registered. Tick loop running.", len(active))

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        log.info("Shutting down…")
        await arbiter.destroy_channel(k.channel)
        await manager.stop()
        log.info("Arbiter stopped.")


if __name__ == "__main__":
    asyncio.run(main())
