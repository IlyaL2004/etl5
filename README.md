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
4. Запускаем spark
```bash
docker exec -it etl5-spark-master-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/main.py
```


## Проверка работы

1. Вставить тестовые данные в PostgreSQL:

```bash
docker exec -it etl5-postgres-1 psql -U user -d mydb


INSERT INTO messages (user_id, track_id, genre, artists, timestamp)
VALUES 
(1, 'track_001', ARRAY['pop', 'electronic'], ARRAY['ArtistA', 'ArtistB'], NOW()),
(2, 'track_002', ARRAY['rock', 'metal'], ARRAY['BandX', 'BandY'], NOW());
```
2. Проверить данные в ClickHouse:

```bash
docker exec -it etl5-clickhouse-1 clickhouse-client

SELECT * FROM test.messages
```

Ожидаемый вывод:
```
┌─user_id─┬─track_id──┬─genre──────┬─artist──┬───────────timestamp─┐
│       1 │ track_001 │ pop        │ ArtistA │ 2025-05-24 17:17:45 │
│       1 │ track_001 │ pop        │ ArtistB │ 2025-05-24 17:17:45 │
│       1 │ track_001 │ electronic │ ArtistA │ 2025-05-24 17:17:45 │
│       1 │ track_001 │ electronic │ ArtistB │ 2025-05-24 17:17:45 │
│       2 │ track_002 │ rock       │ BandX   │ 2025-05-24 17:17:45 │
│       2 │ track_002 │ rock       │ BandY   │ 2025-05-24 17:17:45 │
│       2 │ track_002 │ metal      │ BandX   │ 2025-05-24 17:17:45 │
│       2 │ track_002 │ metal      │ BandY   │ 2025-05-24 17:17:45 │
└─────────┴───────────┴────────────┴─────────┴─────────────────────┘


```

## Архитектура
```
PostgreSQL → (Debezium) → Kafka → Spark Streaming → ClickHouse
```