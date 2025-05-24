# ETL Pipeline with PostgreSQL, Debezium, Kafka, Spark и ClickHouse

Реализация ETL-пайплайна для обработки данных в реальном времени с использованием:

- **PostgreSQL** - источник данных
- **Debezium** - CDC (Change Data Capture)
- **Kafka** - потоковый брокер
- **Spark Structured Streaming** - обработка данных
- **ClickHouse** - аналитическая СУБД

## Предварительные требования

- Docker 20.10+
- Docker Compose 2.20+
- 4 ГБ свободной оперативной памяти
- Порты 5432, 9092, 8083, 8123, 8080 свободны

## Запуск проекта

1. Клонировать репозиторий:
```bash
git clone https://github.com/yourusername/etl5.git
```
2. Перейти в каталог
```bash
cd etl5
```
3. Запустить все сервисы:
```bash
docker-compose up -d
```
4. Создать топик
```bash
docker exec -it etl5-kafka-1 kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
5. Создать таблицу в clickhouse
```bash
docker exec -it etl5-clickhouse-1 clickhouse-client

CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.messages
                (
                    message String,
                    timestamp DateTime
                ) ENGINE = MergeTree()
                ORDER BY timestamp
````
6. Скачать зависимости spark
```bash
docker exec -it etl5-spark-master-1 pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r /app/requirements.txt
```
7. Запустить топик
```bash
docker exec -it etl5-kafka-1 kafka-topics --create --topic pgserver.public.messages --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
8. Создать таблицу в postgres
```bash
docker exec -it etl5-postgres-1 psql -U user -d mydb

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```
9. Запускаем spark
```bash
docker exec -it etl5-spark-master-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/main.py
```
10. Запускаем Debezium
```bash
11.curl -X POST -H "Content-Type: application/json" --data "@connector-config.json" http://localhost:8083/connectors
```

## Проверка работы

1. Вставить тестовые данные в PostgreSQL:

```bash
INSERT INTO messages(id, message, created_at) 
VALUES (13, 'test message', NOW());
```
2. Проверить данные в ClickHouse:

```bash
SELECT * FROM test.messages
```

Ожидаемый вывод:
```
test message    2024-05-28 14:30:45
```

## Архитектура
```
PostgreSQL → (Debezium) → Kafka → Spark Streaming → ClickHouse
```