-- Подключаемся к системной базе 'postgres'
\c postgres

-- Удаляем базу 'mydb', если она существует
DROP DATABASE IF EXISTS mydb;

-- Создаем новую базу 'mydb'
CREATE DATABASE mydb;

-- Переключаемся на созданную базу
\c mydb

-- Создаем таблицу 'messages'
CREATE TABLE messages (
    user_id INT,
    track_id VARCHAR(255),
    genre TEXT[],
    artists TEXT[],
    timestamp TIMESTAMP DEFAULT NOW()
);