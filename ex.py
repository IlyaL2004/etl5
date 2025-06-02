from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
import asyncio
import logging
import psycopg2
import json
import re
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp

from generated.events_pb2 import (
    SessionStarted,
    ChunksAckEvent,
    BitrateChangedEvent,
    OffsetChangedEvent,
    SessionPaused,
    SessionResumed,
    SessionStopped,
    TrackAddedToPlaylist,
    UserRegistered
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("KafkaConsumer")

KAFKA_BROKER = "localhost:29092"
TOPIC = "etl-topic"

PG_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "mydb",
    "user": "user",
    "password": "password"
}


def convert_proto_timestamp(timestamp_proto: Timestamp) -> datetime:
    return datetime.fromtimestamp(timestamp_proto.seconds + timestamp_proto.nanos / 1e9)


def save_to_postgres(table: str, data: dict):
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, list(data.values()))

        conn.commit()
        logger.info(f"Данные сохранены в {table}: {data}")
    except Exception as e:
        logger.error(f"Ошибка записи в PostgreSQL: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


broker = KafkaBroker(KAFKA_BROKER)
app = FastStream(broker)


@broker.subscriber(TOPIC)
async def handle(msg: KafkaMessage):
    try:
        event_type = msg.headers.get("event-type")
        if not event_type:
            logger.warning(f"Message without event-type: {msg}")
            return

        if event_type == "TrackAddedToPlaylist":
            event = TrackAddedToPlaylist()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Track {event.track_id} added to playlist {event.playlist_id} "
                f"by user {event.user_id} at {ts}"
            )
            save_to_postgres("playlist_events", {
                "event_type": event_type,
                "playlist_id": event.playlist_id,
                "track_id": event.track_id,
                "user_id": event.user_id,
                "timestamp": ts
            })

        elif event_type == "UserRegistered":
            event = UserRegistered()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"New user registered: {event.user_id}"
            )
            save_to_postgres("users_events", {
                "event_type": event_type,
                "user_id": event.user_id,
                "timestamp": ts
            })

        elif event_type == "SessionStarted":
            event = SessionStarted()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Session {event.session_id} started for user {event.user_id}, "
                f"track {event.track_id}, bitrate {event.bitrate}"
            )
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "user_id": event.user_id,
                "track_id": event.track_id,
                "bitrate": event.bitrate,
                "timestamp": ts
            })

        elif event_type == "ChunksAckEvent":
            event = ChunksAckEvent()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Session {event.session_id} received ACK for {event.acked_chunk_count} chunks"
            )
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "acked_chunk_count": event.acked_chunk_count,
                "timestamp": ts
            })

        elif event_type == "BitrateChangedEvent":
            event = BitrateChangedEvent()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Session {event.session_id} changed bitrate to {event.new_bitrate}"
            )
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "new_bitrate": event.new_bitrate,
                "timestamp": ts
            })

        elif event_type == "OffsetChangedEvent":
            event = OffsetChangedEvent()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Session {event.session_id} changed offset from {event.old_chunk_offset} "
                f"to {event.new_chunk_offset}"
            )
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "new_chunk_offset": event.new_chunk_offset,
                "old_chunk_offset": event.old_chunk_offset,
                "timestamp": ts
            })

        elif event_type == "SessionPaused":
            event = SessionPaused()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(f"Session {event.session_id} paused")
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "timestamp": ts
            })

        elif event_type == "SessionResumed":
            event = SessionResumed()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(f"Session {event.session_id} resumed")
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "timestamp": ts
            })

        elif event_type == "SessionStopped":
            event = SessionStopped()
            event.ParseFromString(msg.body)
            ts = convert_proto_timestamp(event.timestamp)
            logger.info(
                f"Session {event.session_id} stopped, total chunks sent: {event.total_chunks_sent}"
            )
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event.session_id,
                "total_chunks_sent": event.total_chunks_sent,
                "timestamp": ts
            })

        else:
            logger.warning(f"Unknown event type: {event_type}")

    except Exception as e:
        logger.error(f"Ошибка обработки: {e}", exc_info=True)


async def main():
    await broker.start()
    logger.info("Consumer запущен. Ожидание сообщений...")
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
