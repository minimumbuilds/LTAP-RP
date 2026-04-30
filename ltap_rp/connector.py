"""Arbiter-side Kafka transport: KafkaConnectorManager + KafkaParticipantConnector."""
import asyncio
import logging
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from ltap.models import Bid, BidRequest, BusEvent, TransmissionResponse
from ltap.transport import ParticipantConnector

from .protocol import (
    TOPIC_BID_REQUESTS,
    TOPIC_BIDS,
    TOPIC_CONTROL,
    TOPIC_EVENTS,
    TOPIC_TRANSMISSIONS,
    TOPIC_WIN_SIGNALS,
    correlation_key,
    decode,
    encode,
    routing_key,
)

log = logging.getLogger(__name__)


class KafkaConnectorManager:
    """Shared Kafka producer/consumer and pending-Future registry for the arbiter.

    A single consumer loop reads from TOPIC_BIDS and TOPIC_TRANSMISSIONS and
    resolves the Future registered by whichever connector is waiting for that
    correlation key.
    """

    def __init__(self, bootstrap_servers: str, fetch_max_wait_ms: int = 50) -> None:
        self._bootstrap = bootstrap_servers
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._pending: dict[str, asyncio.Future] = {}
        self._consume_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

        self._consumer = AIOKafkaConsumer(
            TOPIC_BIDS,
            TOPIC_TRANSMISSIONS,
            bootstrap_servers=self._bootstrap,
            group_id="arbiter",
            auto_offset_reset="latest",
            fetch_max_wait_ms=self._fetch_max_wait_ms,
        )
        await self._consumer.start()
        self._consume_task = asyncio.create_task(
            self._consume_loop(), name="connector-consumer"
        )

    async def stop(self) -> None:
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

    async def _consume_loop(self) -> None:
        try:
            async for msg in self._consumer:
                key = msg.key.decode() if msg.key else ""
                fut = self._pending.pop(key, None)
                if fut and not fut.done():
                    fut.set_result(decode(msg.value))
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Connector consumer loop error")

    def register_pending(self, key: str) -> asyncio.Future:
        """Register a Future that will be resolved when a message with this key arrives."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending[key] = fut
        return fut

    async def publish(self, topic: str, key: bytes, value: dict) -> None:
        await self._producer.send_and_wait(topic, key=key, value=encode(value))

    async def publish_control(
        self, channel_id: str, participant_id: str, data: dict
    ) -> None:
        await self._producer.send_and_wait(
            TOPIC_CONTROL,
            key=routing_key(channel_id, participant_id),
            value=encode(data),
        )


class KafkaParticipantConnector(ParticipantConnector):
    """Arbiter-side connector that communicates with a remote participant via Kafka."""

    BID_TIMEOUT = 10.0
    TX_TIMEOUT = 60.0

    def __init__(
        self,
        participant_id: str,
        channel_id: str,
        manager: KafkaConnectorManager,
    ) -> None:
        self._pid = participant_id
        self._channel = channel_id
        self._mgr = manager

    async def request_bid(self, request: BidRequest) -> Optional[Bid]:
        key = correlation_key("bid", self._channel, self._pid, request.tick)
        fut = self._mgr.register_pending(key)
        await self._mgr.publish(
            TOPIC_BID_REQUESTS,
            key=routing_key(self._channel, self._pid),
            value={
                "tick": request.tick,
                "channel_id": request.channel_id,
                "participant_id": request.participant_id,
                "intent_version": request.intent_version,
            },
        )
        try:
            data = await asyncio.wait_for(asyncio.shield(fut), timeout=self.BID_TIMEOUT)
            return Bid.from_dict(data)
        except asyncio.TimeoutError:
            self._mgr._pending.pop(key, None)
            return None

    async def signal_transmission(
        self, channel_id: str, tick: int
    ) -> Optional[TransmissionResponse]:
        key = correlation_key("tx", self._channel, self._pid, tick)
        fut = self._mgr.register_pending(key)
        await self._mgr.publish(
            TOPIC_WIN_SIGNALS,
            key=routing_key(self._channel, self._pid),
            value={"tick": tick, "channel_id": channel_id},
        )
        try:
            data = await asyncio.wait_for(asyncio.shield(fut), timeout=self.TX_TIMEOUT)
            return TransmissionResponse.from_dict(data)
        except asyncio.TimeoutError:
            self._mgr._pending.pop(key, None)
            return None

    async def deliver_event(self, event: BusEvent) -> Optional[Bid]:
        fut: Optional[asyncio.Future] = None
        bid_key: Optional[str] = None

        if event.bid_request is not None:
            bid_key = correlation_key(
                "bid", self._channel, self._pid, event.bid_request.tick
            )
            fut = self._mgr.register_pending(bid_key)

        await self._mgr.publish(
            TOPIC_EVENTS,
            key=routing_key(self._channel, self._pid),
            value=event.to_dict(),
        )

        if fut is not None:
            try:
                data = await asyncio.wait_for(asyncio.shield(fut), timeout=self.BID_TIMEOUT)
                return Bid.from_dict(data)
            except asyncio.TimeoutError:
                if bid_key:
                    self._mgr._pending.pop(bid_key, None)
                return None

        return None
