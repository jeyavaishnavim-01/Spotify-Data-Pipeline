
-- Step 1: Create and select our database
CREATE DATABASE IF NOT EXISTS spotify_db;
USE spotify_db;

-- Artists table
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR(100) PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL,
    genres      TEXT,
    followers   BIGINT       DEFAULT 0,
    popularity  INT          DEFAULT 0,
    artist_url  VARCHAR(500),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Albums table
CREATE TABLE IF NOT EXISTS albums (
    album_id     VARCHAR(100) PRIMARY KEY,
    album_name   VARCHAR(255) NOT NULL,
    artist_id    VARCHAR(100),
    release_date DATE,
    total_tracks INT          DEFAULT 0,
    album_type   VARCHAR(50),
    image_url    VARCHAR(500),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

-- Tracks table (main table)
CREATE TABLE IF NOT EXISTS tracks (
    track_id    VARCHAR(100) PRIMARY KEY,
    track_name  VARCHAR(255) NOT NULL,
    artist_id   VARCHAR(100),
    album_id    VARCHAR(100),
    duration_ms  INT,
    duration_min DECIMAL(5,2),
    popularity   INT          DEFAULT 0,
    explicit     BOOLEAN      DEFAULT FALSE,
    track_url    VARCHAR(500),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY (album_id)  REFERENCES albums(album_id)
);

-- Pipeline run logs
CREATE TABLE IF NOT EXISTS pipeline_logs (
    log_id            INT AUTO_INCREMENT PRIMARY KEY,
    run_date          DATE         NOT NULL,
    tracks_extracted  INT          DEFAULT 0,
    status            VARCHAR(20)  DEFAULT 'success',
    duration_seconds  DECIMAL(8,2)
);

SELECT 'All tables created!' AS result;
