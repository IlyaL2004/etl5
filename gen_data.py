import csv
from datetime import datetime, timedelta
import random
import uuid
import math

# ============================
# Настройки генерации
# ============================
NUM_USERS = 500
NUM_TRACKS = 200
NUM_SESSIONS = 10000
NUM_PLAYLIST_EVENTS = 2000
DAYS_BACK = 30

# ============================
# Вспомогательные функции
# ============================
def random_timestamp(start_days_back=DAYS_BACK):
    now = datetime.now()
    start = now - timedelta(days=start_days_back)
    return start + (now - start) * random.random()

def random_bitrate():
    return random.choice(["64k", "128k", "256k", "320k"])

# Возвращает подтверждённое количество чанков в одном ACK (примерно указанное число, с потерей до 5%)
def random_chunk_ack_for_block(block_size):
    # block_size обычно 10, но может быть меньше (последний фрагмент)
    return int(block_size * random.uniform(0.95, 1.0))


# ============================
# 1) Генерируем список пользователей
# ============================
users = [{
    "user_id_str": str(uuid.uuid4()),
    "user_id_int": random.randint(100000, 999999),
    "email": f"user{idx}@example.com",
    "username": f"user{idx}"
} for idx in range(NUM_USERS)]

# ============================
# 2) Генерируем список треков
# ============================
tracks = [{
    "track_id_str": str(uuid.uuid4()),
    "track_id_int": random.randint(1000, 9999)
} for _ in range(NUM_TRACKS)]

# ============================
# 3) Открываем CSV-файлы для записи
# ============================
with open("users_events.csv", "w", newline="") as user_file, \
     open("session_events.csv", "w", newline="") as session_file, \
     open("playlist_events.csv", "w", newline="") as playlist_file:

    user_writer = csv.writer(user_file)
    session_writer = csv.writer(session_file)
    playlist_writer = csv.writer(playlist_file)

    # ----------------------------
    # Заголовки для users_events.csv
    # ----------------------------
    user_writer.writerow(["event_id", "event_type", "user_id", "timestamp"])

    # ----------------------------
    # Заголовки для session_events.csv
    # ----------------------------
    session_writer.writerow([
        "event_id", "event_type", "session_id", "user_id", "track_id", "bitrate",
        "new_bitrate", "acked_chunk_count", "total_chunks_sent",
        "new_chunk_offset", "old_chunk_offset", "timestamp"
    ])

    # ----------------------------
    # Заголовки для playlist_events.csv
    # ----------------------------
    playlist_writer.writerow(["event_id", "event_type", "playlist_id", "track_id", "user_id", "timestamp"])

    event_counter = 1

    # ============================
    # 4) Генерируем события регистрации пользователей
    # ============================
    for user in users:
        user_writer.writerow([
            event_counter,
            "UserRegistered",
            user["user_id_str"],
            random_timestamp().strftime("%Y-%m-%d %H:%M:%S")
        ])
        event_counter += 1

    # ============================
    # 5) Генерируем события для каждой сессии
    # ============================
    for _ in range(NUM_SESSIONS):
        user = random.choice(users)
        track = random.choice(tracks)
        session_id = str(uuid.uuid4())
        start_time = random_timestamp()
        session_duration = random.randint(60, 3600)  # длительность сессии в секундах
        end_time = start_time + timedelta(seconds=session_duration)

        # 5.1) Определяем полный размер трека в чанках (кратный 10, от 100 до 500)
        track_length_chunks = random.randint(10, 50) * 10

        # 5.2) Определяем, сколько фактически прослушано пользователем (между 50% и 100%)
        listened_chunks = int(track_length_chunks * random.uniform(0.5, 1.0))
        if listened_chunks == 0:
            listened_chunks = 1  # минимум 1 чанк

        # Сколько полных 10-чанковых блоков и есть ли остаток
        full_blocks = listened_chunks // 10
        remainder = listened_chunks % 10
        num_ack_events = full_blocks + (1 if remainder > 0 else 0)

        # ---- SessionStarted ----
        initial_bitrate = random_bitrate()
        session_writer.writerow([
            event_counter,
            "SessionStarted",
            session_id,
            user["user_id_str"],
            track["track_id_str"],
            initial_bitrate,  # стартовый битрейт
            "",  # new_bitrate
            "",  # acked_chunk_count
            "",  # total_chunks_sent
            "",  # new_chunk_offset
            "",  # old_chunk_offset
            start_time.strftime("%Y-%m-%d %H:%M:%S")
        ])
        event_counter += 1

        # 5.3) Генерируем таймстампы для ChunksAckEvent (num_ack_events штук)
        ack_times = sorted([
            start_time + timedelta(seconds=random.randint(1, session_duration - 1))
            for _ in range(num_ack_events)
        ])

        # 5.4) Генерируем таймстампы для прочих событий (2–20 штук)
        other_event_count = random.randint(2, 20)
        other_times = [
            start_time + timedelta(seconds=random.randint(1, session_duration - 1))
            for _ in range(other_event_count)
        ]

        # 5.5) Собираем все промежуточные события в единый список
        events = []
        blocks_generated = 0  # сколько блоков ACK уже сгенерировано

        # 5.5.1) Добавляем ChunksAckEvent (каждый хранит только подтверждённые чанки за этот блок)
        # Сначала генерируем для полных блоков по 10 чанков, потом – для остаточного блока (если есть)
        for idx, ts in enumerate(ack_times):
            if idx < full_blocks:
                block_size = 10
            else:
                block_size = remainder

            # Подтверждённые чанки в этом событии (с потерей до 5%)
            acked = random_chunk_ack_for_block(block_size)
            if acked > block_size:
                acked = block_size

            events.append((ts, "ChunksAckEvent", {"acked_chunk_count": acked}))
            blocks_generated += 1

        # 5.5.2) Добавляем остальные события (BitrateChangedEvent, SessionPaused, SessionResumed, OffsetChangedEvent)
        current_offset = 0
        for ts in other_times:
            event_type = random.choices(
                ["BitrateChangedEvent", "SessionPaused", "SessionResumed", "OffsetChangedEvent"],
                weights=[0.167, 0.167, 0.167, 0.499],
                k=1
            )[0]

            if event_type == "BitrateChangedEvent":
                new_bitrate = random.choice([64, 128, 256, 320])
                events.append((ts, event_type, {"new_bitrate": new_bitrate}))

            elif event_type == "OffsetChangedEvent":
                # Новый оффсет не может превышать полный размер трека (track_length_chunks),
                # но может быть в любом месте внутри трека, даже если пользователь не дошёл до конца.
                new_offset = random.randint(0, track_length_chunks)
                events.append((ts, event_type, {
                    "new_chunk_offset": new_offset,
                    "old_chunk_offset": current_offset
                }))
                current_offset = new_offset

            else:
                # SessionPaused или SessionResumed
                events.append((ts, event_type, {}))

        # 5.6) Сортируем все промежуточные события (ACK + прочие) по таймстампу
        events.sort(key=lambda x: x[0])

        # 5.7) Записываем каждое промежуточное событие в session_events.csv
        for ts, event_type, data in events:
            if event_type == "ChunksAckEvent":
                session_writer.writerow([
                    event_counter,
                    event_type,
                    session_id,
                    "",  # user_id
                    "",  # track_id
                    "",  # bitrate
                    "",  # new_bitrate
                    data["acked_chunk_count"],  # подтверждённые чанки за этот блок
                    "",  # total_chunks_sent
                    "",  # new_chunk_offset
                    "",  # old_chunk_offset
                    ts.strftime("%Y-%m-%d %H:%M:%S")
                ])

            elif event_type == "BitrateChangedEvent":
                session_writer.writerow([
                    event_counter,
                    event_type,
                    session_id,
                    "",  # user_id
                    "",  # track_id
                    "",  # bitrate
                    data["new_bitrate"],  # новый битрейт
                    "",  # acked_chunk_count
                    "",  # total_chunks_sent
                    "",  # new_chunk_offset
                    "",  # old_chunk_offset
                    ts.strftime("%Y-%m-%d %H:%M:%S")
                ])

            elif event_type in ["SessionPaused", "SessionResumed"]:
                session_writer.writerow([
                    event_counter,
                    event_type,
                    session_id,
                    "",  # user_id
                    "",  # track_id
                    "",  # bitrate
                    "",  # new_bitrate
                    "",  # acked_chunk_count
                    "",  # total_chunks_sent
                    "",  # new_chunk_offset
                    "",  # old_chunk_offset
                    ts.strftime("%Y-%m-%d %H:%M:%S")
                ])

            elif event_type == "OffsetChangedEvent":
                session_writer.writerow([
                    event_counter,
                    event_type,
                    session_id,
                    "",  # user_id
                    "",  # track_id
                    "",  # bitrate
                    "",  # new_bitrate
                    "",  # acked_chunk_count
                    "",  # total_chunks_sent
                    data["new_chunk_offset"],  # новое смещение (в чанках)
                    data["old_chunk_offset"],  # старое смещение
                    ts.strftime("%Y-%m-%d %H:%M:%S")
                ])

            event_counter += 1

        # 5.8) В конце сессии: SessionStopped с total_chunks_sent = listened_chunks
        session_writer.writerow([
            event_counter,
            "SessionStopped",
            session_id,
            user["user_id_str"],
            track["track_id_str"],
            "",  # bitrate
            "",  # new_bitrate
            "",  # acked_chunk_count
            listened_chunks,  # всего отправлено чанков (учтено, что пользователь мог не дослушать)
            "",  # new_chunk_offset
            "",  # old_chunk_offset
            end_time.strftime("%Y-%m-%d %H:%M:%S")
        ])
        event_counter += 1

    # ============================
    # 6) Генерируем события плейлистов
    # ============================
    for _ in range(NUM_PLAYLIST_EVENTS):
        user = random.choice(users)
        track = random.choice(tracks)
        playlist_writer.writerow([
            event_counter,
            "TrackAddedToPlaylist",
            random.randint(1, 100),       # playlist_id (случайный)
            track["track_id_int"],        # track_id (integer)
            user["user_id_int"],          # user_id (integer)
            random_timestamp().strftime("%Y-%m-%d %H:%M:%S")
        ])
        event_counter += 1

print("Генерация данных завершена!")
print(f"Событий создано: {event_counter - 1}")
print("Файлы: users_events.csv, session_events.csv, playlist_events.csv")
