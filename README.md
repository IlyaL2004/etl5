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

## Проверка работы

1. Вставить тестовые данные в PostgreSQL:

```bash
docker exec -it etl5-postgres-1 psql -U user -d mydb -c "INSERT INTO messages(message) VALUES ('Hello World!');"
```
2. Проверить данные в ClickHouse:

```bash
docker exec -it etl5-clickhouse-1 clickhouse-client --query "SELECT * FROM test.messages"
```

Ожидаемый вывод:
```
Hello World!    2024-05-28 14:30:45
```

## Архитектура
```
PostgreSQL → (Debezium) → Kafka → Spark Streaming → ClickHouse
```