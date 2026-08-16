CREATE DATABASE IF NOT EXISTS reports_db;

CREATE TABLE IF NOT EXISTS reports_db.prothesis_telemetry_leg (
        timestamp DateTime,
        prothesis_id String,
        user_id String,
        user_email String,
        user_first_name String,
        user_last_name String,
        user_full_name String,
        battery_level Float32,
        temperature Float32,
        speed Float32,
        pressure_heel Float32,
        pressure_toe Float32,
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (user_email, prothesis_id, timestamp)
    SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS reports_db.prothesis_telemetry_arm (
        timestamp DateTime,
        prothesis_id String,
        user_id String,
        user_email String,
        user_first_name String,
        user_last_name String,
        user_full_name String,
        battery_level Float32,
        temperature Float32,
        grip_strength Float32,
        wrist_rotation Float32,
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (user_email, prothesis_id, timestamp)
    SETTINGS index_granularity = 8192;