"""
Cleans raw JSON into structured DataFrames
"""
import json, logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.config import RAW_DATA_PATH, TRANSFORMED_DATA_PATH

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('transformer')

class SpotifyTransformer:
    def __init__(self, run_date=None):
        self.run_date = run_date or datetime.now().strftime('%Y-%m-%d')
        self.raw_dir  = Path(RAW_DATA_PATH) / self.run_date
        self.out_dir  = Path(TRANSFORMED_DATA_PATH) / self.run_date
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def _load(self, name):
        with open(self.raw_dir / f'{name}.json') as f:
            return json.load(f)

    def _parse_date(self, value):
        if not value:
            return None
        for fmt in ('%Y-%m-%d', '%Y-%m', '%Y'):
            try:
                return datetime.strptime(value, fmt).date()
            except:
                continue
        return None

    def transform_artists(self, raw):
        rows = []
        for a in raw:
            if not a or not a.get('id'):
                continue
            rows.append({
                'artist_id'  : a['id'],
                'artist_name': a.get('name', 'Unknown').strip(),
                'genres'     : ', '.join(a.get('genres', [])),
                'followers'  : a.get('followers', {}).get('total', 0),
                'popularity' : a.get('popularity', 0),
                'artist_url' : a.get('external_urls', {}).get('spotify'),
            })
        df = pd.DataFrame(rows).drop_duplicates('artist_id')
        df['popularity'] = df['popularity'].clip(0, 100)
        logger.info(f'Artists: {len(df)} rows')
        return df

    def transform_albums(self, raw):
        rows = []
        for al in raw:
            if not al or not al.get('id'):
                continue
            rows.append({
                'album_id'    : al['id'],
                'album_name'  : al.get('name', 'Unknown').strip(),
                'artist_id'   : al['artists'][0]['id'] if al.get('artists') else None,
                'release_date': self._parse_date(al.get('release_date', '')),
                'total_tracks': al.get('total_tracks', 0),
                'album_type'  : al.get('album_type', 'album'),
                'image_url'   : (al.get('images') or [{}])[0].get('url'),
            })
        df = pd.DataFrame(rows).drop_duplicates('album_id')
        logger.info(f'Albums: {len(df)} rows')
        return df

    def transform_tracks(self, raw):
        rows = []
        for t in raw:
            if not t or not t.get('id'):
                continue
            ms = t.get('duration_ms', 0) or 0
            rows.append({
                'track_id'   : t['id'],
                'track_name' : t.get('name', 'Unknown').strip(),
                'artist_id'  : t['artists'][0]['id'] if t.get('artists') else None,
                'album_id'   : t['album']['id'] if t.get('album') else None,
                'duration_ms' : ms,
                'duration_min': round(ms / 60000, 2),
                'popularity'  : t.get('popularity', 0),
                'explicit'    : bool(t.get('explicit', False)),
                'track_url'   : t.get('href'),
            })
        df = pd.DataFrame(rows).drop_duplicates('track_id')
        df = df[df['duration_ms'] > 0]
        df['popularity'] = df['popularity'].clip(0, 100)
        logger.info(f'Tracks: {len(df)} rows')
        return df

    def run(self):
        artists_df = self.transform_artists(
            self._load('raw_artists')['artists'])
        albums_df  = self.transform_albums(
            self._load('raw_albums')['albums'])
        tracks_df  = self.transform_tracks(
            self._load('raw_tracks')['tracks'])
        artists_df.to_csv(self.out_dir / 'artists.csv', index=False)
        albums_df.to_csv( self.out_dir / 'albums.csv',  index=False)
        tracks_df.to_csv( self.out_dir / 'tracks.csv',  index=False)
        logger.info('All CSVs saved')
        return {'dataframes': {
            'artists': artists_df,
            'albums' : albums_df,
            'tracks' : tracks_df}}

if __name__ == '__main__':
    result = SpotifyTransformer().run()
    for name, df in result['dataframes'].items():
        print(f'{name}: {len(df)} rows')
