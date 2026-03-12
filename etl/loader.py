"""
Loads DataFrames into MySQL using upsert
"""
import logging, time
from datetime import datetime
import mysql.connector
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.config import DB_CONFIG

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('loader')

class SpotifyLoader:
    def connect(self):
        self.conn   = mysql.connector.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        logger.info('MySQL connected')

    def disconnect(self):
        self.cursor.close()
        self.conn.close()
        logger.info('MySQL disconnected')

    def load_artists(self, df):
        sql = '''
            INSERT INTO artists
            (artist_id, artist_name, genres, followers, popularity, artist_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                artist_name = VALUES(artist_name),
                genres      = VALUES(genres),
                followers   = VALUES(followers),
                popularity  = VALUES(popularity)
        '''
        rows = [(r.artist_id, r.artist_name, r.genres,
                 int(r.followers), int(r.popularity),
                 r.artist_url if pd.notna(str(r.artist_url)) else None)
                for r in df.itertuples(index=False)]
        self.cursor.executemany(sql, rows)
        self.conn.commit()
        logger.info(f'Loaded {len(rows)} artists')

    def load_albums(self, df):
        sql = '''
            INSERT INTO albums
            (album_id, album_name, artist_id, release_date,
             total_tracks, album_type, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                album_name = VALUES(album_name)
        '''
        rows = [(r.album_id, r.album_name, r.artist_id,
                 r.release_date if pd.notna(str(r.release_date)) else None,
                 int(r.total_tracks), r.album_type, r.image_url)
                for r in df.itertuples(index=False)]
        self.cursor.executemany(sql, rows)
        self.conn.commit()
        logger.info(f'Loaded {len(rows)} albums')

    def load_tracks(self, df):
        sql = '''
            INSERT INTO tracks
            (track_id, track_name, artist_id, album_id,
             duration_ms, duration_min, popularity, explicit, track_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                track_name = VALUES(track_name),
                popularity = VALUES(popularity)
        '''
        rows = [(r.track_id, r.track_name, r.artist_id, r.album_id,
                 int(r.duration_ms), float(r.duration_min),
                 int(r.popularity), bool(r.explicit), r.track_url)
                for r in df.itertuples(index=False)]
        self.cursor.executemany(sql, rows)
        self.conn.commit()
        logger.info(f'Loaded {len(rows)} tracks')
        return len(rows)

    def log_run(self, run_date, count, status, duration):
        self.cursor.execute(
            '''INSERT INTO pipeline_logs
               (run_date, tracks_extracted, status, duration_seconds)
               VALUES (%s, %s, %s, %s)''',
            (run_date, count, status, round(duration, 2)))
        self.conn.commit()

    def run(self, artists_df, albums_df, tracks_df, run_date=None):
        run_date = run_date or datetime.now().strftime('%Y-%m-%d')
        start    = time.time()
        self.connect()
        try:
            self.load_artists(artists_df)
            self.load_albums(albums_df)
            loaded = self.load_tracks(tracks_df)
            status = 'success'
        except Exception as e:
            status = 'failed'
            logger.error(f'Load failed: {e}')
            raise
        finally:
            self.log_run(run_date, len(tracks_df),
                         status, time.time() - start)
            self.disconnect()
        return {'status': status, 'loaded': loaded}
