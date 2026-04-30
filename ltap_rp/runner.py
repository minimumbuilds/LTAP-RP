"""Participant-side Kafka runner: listens for LTAP messages and drives an LTAPParticipant."""
import asyncio
import logging
import time
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from ltap.models import Bid, BidRequest, BusEvent, TransmissionResponse
from ltap.participant import LTAPParticipant

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


class KafkaParticipantRunner:
    """Wraps an LTAPParticipant and drives it via Kafka messages.

    Startup: waits for a registration ack on TOPIC_CONTROL (reading from the
    beginning of the topic so it doesn't miss an ack published before this
    runner started).  Then opens the main protocol consumer loop.
    """

    def __init__(
        self,
        participant: LTAPParticipant,
        channel_id: str,
        bootstrap_servers: str,
        fetch_max_wait_ms: int = 50,
    ) -> None:
        self._participant = participant
        self._channel = channel_id
        self._bootstrap = bootstrap_servers
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._pid = participant.participant_id

    async def run(self) -> None:
        producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await producer.start()
        try:
            await self._wait_for_registration()
            await self._protocol_loop(producer)
        except asyncio.CancelledError:
            pass
        finally:
            await producer.stop()

    async def _wait_for_registration(self) -> None:
        # Unique group so each restart reads from the beginning of TOPIC_CONTROL.
        group = f"ctrl-{self._pid}-{int(time.time() * 1000)}"
        consumer = AIOKafkaConsumer(
            TOPIC_CONTROL,
            bootstrap_servers=self._bootstrap,
            group_id=group,
            auto_offset_reset="earliest",
            fetch_max_wait_ms=self._fetch_max_wait_ms,
        )
        await consumer.start()
        target = routing_key(self._channel, self._pid)
        log.info("[%s] waiting for registration ack…", self._pid)
        try:
            async for msg in consumer:
                if msg.key != target:
                    continue
                data = decode(msg.value)
                if data.get("type") == "registered":
                    log.info(
                        "[%s] registered on channel '%s', members=%s",
                        self._pid,
                        self._channel,
                        data.get("participants"),
                    )
                    return
        finally:
            await consumer.stop()

    async def _protocol_loop(self, producer: AIOKafkaProducer) -> None:
        consumer = AIOKafkaConsumer(
            TOPIC_BID_REQUESTS,
            TOPIC_WIN_SIGNALS,
            TOPIC_EVENTS,
            bootstrap_servers=self._bootstrap,
            group_id=f"participant-{self._pid}",
            auto_offset_reset="latest",
            fetch_max_wait_ms=self._fetch_max_wait_ms,
        )
        await consumer.start()
        target = routing_key(self._channel, self._pid)
        log.info("[%s] protocol loop started", self._pid)
        try:
            async for msg in consumer:
                if msg.key != target:
                    continue
                data = decode(msg.value)
                if msg.topic == TOPIC_BID_REQUESTS:
                    await self._handle_bid_request(data, producer)
                elif msg.topic == TOPIC_WIN_SIGNALS:
                    await self._handle_win_signal(data, producer)
                elif msg.topic == TOPIC_EVENTS:
                    await self._handle_event(data, producer)
        except asyncio.CancelledError:
            pass
        finally:
            await consumer.stop()

    async def _handle_bid_request(
        self, data: dict, producer: AIOKafkaProducer
    ) -> None:
        request = BidRequest(
            tick=data["tick"],
            channel_id=data["channel_id"],
            participant_id=data["participant_id"],
            intent_version=data["intent_version"],
        )
        bid = await self._participant.generate_bid(request)
        key = correlation_key("bid", self._channel, self._pid, request.tick)
        await producer.send_and_wait(
            TOPIC_BIDS, key=key.encode(), value=encode(bid.to_dict())
        )
        log.debug("[%s] bid tick=%d priority=%.2f", self._pid, request.tick, bid.priority)

    async def _handle_win_signal(
        self, data: dict, producer: AIOKafkaProducer
    ) -> None:
        tick = data["tick"]
        channel_id = data["channel_id"]
        response = await self._participant.generate_transmission(channel_id)
        if response is None:
            response = TransmissionResponse(content="[no response]")
        key = correlation_key("tx", self._channel, self._pid, tick)
        await producer.send_and_wait(
            TOPIC_TRANSMISSIONS, key=key.encode(), value=encode(response.to_dict())
        )
        log.info("[%s] transmitted tick=%d: %s", self._pid, tick, response.content[:120])

    async def _handle_event(self, data: dict, producer: AIOKafkaProducer) -> None:
        bid_request_data: Optional[dict] = data.get("bid_request")
        bid_request: Optional[BidRequest] = None
        if bid_request_data:
            bid_request = BidRequest(
                tick=bid_request_data["tick"],
                channel_id=bid_request_data["channel_id"],
                participant_id=bid_request_data["participant_id"],
                intent_version=bid_request_data["intent_version"],
            )

        event = BusEvent(
            type=data["type"],
            tick=data["tick"],
            channel_id=data["channel_id"],
            sender=data.get("sender"),
            content=data["content"],
            addressed_to=data.get("addressed_to"),
            is_self=data.get("is_self", False),
            bid_request=bid_request,
        )

        await self._participant.on_event(event)

        if bid_request is not None:
            bid = await self._participant.generate_bid(bid_request)
            key = correlation_key("bid", self._channel, self._pid, bid_request.tick)
            await producer.send_and_wait(
                TOPIC_BIDS, key=key.encode(), value=encode(bid.to_dict())
            )
            log.debug(
                "[%s] bundled bid tick=%d priority=%.2f",
                self._pid,
                bid_request.tick,
                bid.priority,
            )
