from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
import asyncio
import logging
import psycopg2
import json
import re
from datetime import datetime

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

broker = KafkaBroker(KAFKA_BROKER)
app = FastStream(broker)


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


def parse_old_format(body: str):
    """Парсинг сообщений старого формата"""
    pattern = r"\((\d+),\s*'([^']*)',\s*ARRAY\[([^\]]*)\],\s*ARRAY\[([^\]]*)\],\s*(NOW\(\)|'[^']*')\)"
    match = re.search(pattern, body)
    if match:
        user_id = match.group(1)
        track_id = match.group(2)

        # Обработка жанров
        genres_str = match.group(3).replace("'", "")
        genres = [g.strip() for g in genres_str.split(',') if g.strip()]

        # Обработка артистов
        artists_str = match.group(4).replace("'", "")
        artists = [a.strip() for a in artists_str.split(',') if a.strip()]

        # Обработка временной метки
        time_val = match.group(5)
        if time_val == "NOW()":
            timestamp = datetime.now()
        else:
            try:
                time_val = time_val.strip("'")
                timestamp = datetime.fromisoformat(time_val)
            except:
                timestamp = datetime.now()

        return {
            "type": "old_format",
            "data": {
                "user_id": int(user_id),
                "track_id": track_id,
                "genre": genres,
                "artists": artists,
                "timestamp": timestamp
            }
        }
    return None


def parse_header_format(body: str):
    """Парсинг сообщений с event-type в теле"""
    # Формат: event-type:UserRegistered;{json}
    if ';' not in body:
        return None

    header_part, json_part = body.split(';', 1)

    if not header_part.startswith('event-type:'):
        return None

    event_type = header_part.split(':', 1)[1].strip()

    try:
        data = json.loads(json_part)
        return {
            "type": "header_format",
            "event_type": event_type,
            "data": data
        }
    except json.JSONDecodeError:
        return None


def save_event(event_type: str, event_data: dict):
    """Сохраняет событие в соответствующую таблицу"""
    try:
        if event_type == "UserRegistered":
            save_to_postgres("user_events", {
                "event_type": event_type,
                "user_id": event_data.get("user_id"),
                "email": event_data.get("email"),
                "username": event_data.get("username"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "SessionStarted":
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "user_id": event_data.get("user_id"),
                "track_id": event_data.get("track_id"),
                "bitrate": event_data.get("bitrate"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "BitrateChangedEvent":
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "new_bitrate": event_data.get("new_bitrate"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "ChunksAckEvent":
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "acked_chunk_count": event_data.get("acked_chunk_count"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type in ("SessionPaused", "SessionResumed"):
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "SessionStopped":
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "total_chunks_sent": event_data.get("total_chunks_sent"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "OffsetChangedEvent":
            save_to_postgres("session_events", {
                "event_type": event_type,
                "session_id": event_data.get("session_id"),
                "new_chunk_offset": event_data.get("new_chunk_offset"),
                "old_chunk_offset": event_data.get("old_chunk_offset"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        elif event_type == "TrackAddedToPlaylist":
            save_to_postgres("playlist_events", {
                "event_type": event_type,
                "playlist_id": event_data.get("playlist_id"),
                "track_id": event_data.get("track_id"),
                "user_id": event_data.get("user_id"),
                "timestamp": datetime.fromisoformat(event_data.get("timestamp"))
            })

        else:
            logger.warning(f"Неизвестный тип события: {event_type}")
    except Exception as e:
        logger.error(f"Ошибка сохранения события {event_type}: {e}", exc_info=True)


@broker.subscriber(TOPIC)
async def handle(msg: KafkaMessage):
    try:
        # Извлекаем заголовки
        headers = {k: v.decode() if isinstance(v, bytes) else v for k, v in (msg.headers or {}).items()}
        event_type = headers.get("event-type")

        # Декодируем тело сообщения
        body = msg.body.decode()
        logger.info(f"Получено сообщение: event_type={event_type}, body={body}")

        # Пытаемся распарсить тело как JSON
        json_body = None
        try:
            json_body = json.loads(body)
        except json.JSONDecodeError:
            pass

        # Обработка различных форматов сообщений
        processed = False

        # 1. Новый формат с заголовком event-type и JSON телом
        if event_type and json_body:
            processed = True
            save_event(event_type, json_body)

        # 2. Старый формат сообщений
        if not processed:
            old_format = parse_old_format(body)
            if old_format:
                processed = True
                save_to_postgres("messages", old_format["data"])

        # 3. Формат с event-type в теле сообщения
        if not processed:
            header_format = parse_header_format(body)
            if header_format:
                processed = True
                save_event(header_format["event_type"], header_format["data"])

        # 4. Формат с JSON-телом без заголовков
        if not processed and json_body:
            # Если в JSON есть поле event_type
            if "event_type" in json_body:
                processed = True
                save_event(json_body["event_type"], json_body)
            # Если в JSON есть поле type (альтернативное название)
            elif "type" in json_body:
                processed = True
                save_event(json_body["type"], json_body)

        # 5. Формат с event-type в начале тела (без JSON)
        if not processed and ':' in body:
            parts = body.split(':', 1)
            potential_type = parts[0].strip()
            if potential_type in [
                "UserRegistered", "SessionStarted", "BitrateChangedEvent",
                "ChunksAckEvent", "SessionPaused", "SessionResumed",
                "SessionStopped", "OffsetChangedEvent", "TrackAddedToPlaylist"
            ]:
                processed = True
                logger.warning(f"Необработанный формат с известным типом: {body}")
                # Здесь можно добавить парсинг для этого формата

        if not processed:
            logger.warning(f"Неизвестный формат сообщения: {body}")

    except Exception as e:
        logger.error(f"Ошибка обработки: {e}", exc_info=True)


async def main():
    await broker.start()
    logger.info("Consumer запущен. Ожидание сообщений...")
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())