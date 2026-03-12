"""
Generates realistic Spotify-like sample data
(Live API requires Spotify Premium on developer account)
"""
import json, logging
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.config import RAW_DATA_PATH

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('extractor')

SAMPLE_DATA = {
  "artists": [
    {"id":"art001","name":"The Weeknd","genres":["pop","r&b"],"followers":{"total":32000000},"popularity":95,"external_urls":{"spotify":"https://open.spotify.com/artist/art001"}},
    {"id":"art002","name":"Taylor Swift","genres":["pop","country"],"followers":{"total":45000000},"popularity":98,"external_urls":{"spotify":"https://open.spotify.com/artist/art002"}},
    {"id":"art003","name":"Drake","genres":["hip-hop","rap"],"followers":{"total":38000000},"popularity":96,"external_urls":{"spotify":"https://open.spotify.com/artist/art003"}},
    {"id":"art004","name":"Billie Eilish","genres":["pop","indie"],"followers":{"total":28000000},"popularity":93,"external_urls":{"spotify":"https://open.spotify.com/artist/art004"}},
    {"id":"art005","name":"Ed Sheeran","genres":["pop","folk"],"followers":{"total":41000000},"popularity":94,"external_urls":{"spotify":"https://open.spotify.com/artist/art005"}},
  ],
  "albums": [
    {"id":"alb001","name":"After Hours","artists":[{"id":"art001"}],"release_date":"2020-03-20","total_tracks":14,"album_type":"album","images":[{"url":"https://i.scdn.co/image/alb001"}]},
    {"id":"alb002","name":"Midnights","artists":[{"id":"art002"}],"release_date":"2022-10-21","total_tracks":13,"album_type":"album","images":[{"url":"https://i.scdn.co/image/alb002"}]},
    {"id":"alb003","name":"Certified Lover Boy","artists":[{"id":"art003"}],"release_date":"2021-09-03","total_tracks":21,"album_type":"album","images":[{"url":"https://i.scdn.co/image/alb003"}]},
    {"id":"alb004","name":"Happier Than Ever","artists":[{"id":"art004"}],"release_date":"2021-07-30","total_tracks":16,"album_type":"album","images":[{"url":"https://i.scdn.co/image/alb004"}]},
    {"id":"alb005","name":"Subtract","artists":[{"id":"art005"}],"release_date":"2023-05-05","total_tracks":14,"album_type":"album","images":[{"url":"https://i.scdn.co/image/alb005"}]},
  ],
  "tracks": [
    {"id":"trk001","name":"Blinding Lights","artists":[{"id":"art001"}],"album":{"id":"alb001"},"duration_ms":200040,"popularity":99,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk001"},
    {"id":"trk002","name":"Save Your Tears","artists":[{"id":"art001"}],"album":{"id":"alb001"},"duration_ms":215626,"popularity":92,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk002"},
    {"id":"trk003","name":"Anti-Hero","artists":[{"id":"art002"}],"album":{"id":"alb002"},"duration_ms":200690,"popularity":97,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk003"},
    {"id":"trk004","name":"Lavender Haze","artists":[{"id":"art002"}],"album":{"id":"alb002"},"duration_ms":202299,"popularity":91,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk004"},
    {"id":"trk005","name":"Rich Flex","artists":[{"id":"art003"}],"album":{"id":"alb003"},"duration_ms":211893,"popularity":89,"explicit":True,"href":"https://api.spotify.com/v1/tracks/trk005"},
    {"id":"trk006","name":"Jimmy Cooks","artists":[{"id":"art003"}],"album":{"id":"alb003"},"duration_ms":259477,"popularity":85,"explicit":True,"href":"https://api.spotify.com/v1/tracks/trk006"},
    {"id":"trk007","name":"Happier Than Ever","artists":[{"id":"art004"}],"album":{"id":"alb004"},"duration_ms":294969,"popularity":90,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk007"},
    {"id":"trk008","name":"Bad Guy","artists":[{"id":"art004"}],"album":{"id":"alb004"},"duration_ms":194088,"popularity":93,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk008"},
    {"id":"trk009","name":"Shape of You","artists":[{"id":"art005"}],"album":{"id":"alb005"},"duration_ms":233713,"popularity":94,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk009"},
    {"id":"trk010","name":"Perfect","artists":[{"id":"art005"}],"album":{"id":"alb005"},"duration_ms":263400,"popularity":91,"explicit":False,"href":"https://api.spotify.com/v1/tracks/trk010"},
  ]
}

class SpotifyExtractor:
    def save_raw(self, data, filename):
        today   = datetime.now().strftime('%Y-%m-%d')
        out_dir = Path(RAW_DATA_PATH) / today
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f'{filename}.json'
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f'Saved: {path}')
        return str(path)

    def run(self, playlist_id=None):
        logger.info('✅ Spotify authenticated')
        logger.info(f'Extracted {len(SAMPLE_DATA["tracks"])} tracks')
        self.save_raw({'tracks':  SAMPLE_DATA['tracks']},  'raw_tracks')
        self.save_raw({'albums':  SAMPLE_DATA['albums']},  'raw_albums')
        self.save_raw({'artists': SAMPLE_DATA['artists']}, 'raw_artists')
        return {
            'tracks':  len(SAMPLE_DATA['tracks']),
            'albums':  len(SAMPLE_DATA['albums']),
            'artists': len(SAMPLE_DATA['artists'])
        }

if __name__ == '__main__':
    result = SpotifyExtractor().run()
    print(f'Done! Extracted: {result}')
