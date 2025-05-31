from faststream import FastStream
from faststream.kafka import KafkaBroker
import asyncio
import logging
import psycopg2
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
    "port": 5433,
    "database": "mydb",
    "user": "user",
    "password": "password"
}

broker = KafkaBroker(KAFKA_BROKER)
app = FastStream(broker)


def parse_message(msg: str) -> tuple:
    try:
        # Удаляем технические части строки
        cleaned = re.sub(r'ARRAY|NOW\(\)', '', msg)

        # Извлекаем значения с помощью регулярного выражения
        pattern = r'\((\d+),\s*\'([^\']+)\',\s*\[([^\]]+)\],\s*\[([^\]]+)\]'
        match = re.search(pattern, cleaned)

        if not match:
            logger.error("Не удалось распарсить сообщение")
            return None

        user_id = int(match.group(1))
        track_id = match.group(2)
        genres = [g.strip().strip("'") for g in match.group(3).split(',')]
        artists = [a.strip().strip("'") for a in match.group(4).split(',')]
        timestamp = datetime.now()

        return (user_id, track_id, genres, artists, timestamp)

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        return None


def save_to_postgres(data: tuple):
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO messages (user_id, track_id, genre, artists, timestamp) "
            "VALUES (%s, %s, %s, %s, %s)",
            data
        )
        conn.commit()
        logger.info(f"Данные сохранены в PostgreSQL: {data}")
    except Exception as e:
        logger.error(f"Ошибка записи в PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()


@broker.subscriber(TOPIC)
async def handle(msg: bytes):
    try:
        message = msg.decode('utf-8')
        logger.info(f"Получено сообщение: {message}")

        parsed = parse_message(message)
        if parsed:
            save_to_postgres(parsed)
    except Exception as e:
        logger.error(f"Ошибка обработки: {e}")


async def main():
    await broker.start()
    logger.info("Consumer запущен. Ожидание сообщений...")
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())