"""
Runs the full Spotify ETL pipeline

"""
import sys, logging, time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etl.extractor  import SpotifyExtractor
from etl.transformer import SpotifyTransformer
from etl.loader     import SpotifyLoader

Path('logs').mkdir(exist_ok=True)

logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s [%(levelname)s] %(message)s',
    handlers = [
        logging.StreamHandler(),                           # terminal
        logging.FileHandler('logs/pipeline.log', mode='a') # log file
    ]
)
logger = logging.getLogger('pipeline')

def run_pipeline():
    start = time.time()
    logger.info('=' * 55)
    logger.info('   SPOTIFY DATA PIPELINE — STARTING')
    logger.info('=' * 55)

    # ── STEP 1: EXTRACT ──
    logger.info('--- STEP 1/3: EXTRACT ---')
    ex_result = SpotifyExtractor().run()
    logger.info(f'Extracted: {ex_result}')

    # ── STEP 2: TRANSFORM ──
    logger.info('--- STEP 2/3: TRANSFORM ---')
    tr_result = SpotifyTransformer().run()
    dfs       = tr_result['dataframes']

    # ── STEP 3: LOAD ──
    logger.info('--- STEP 3/3: LOAD ---')
    ld_result = SpotifyLoader().run(
        artists_df = dfs['artists'],
        albums_df  = dfs['albums'],
        tracks_df  = dfs['tracks'],
    )

    total = round(time.time() - start, 1)
    logger.info('=' * 55)
    logger.info(f'  PIPELINE COMPLETE in {total}s')
    logger.info(f'  Loaded: {ld_result["loaded"]} tracks')
    logger.info('=' * 55)

if __name__ == '__main__':
    run_pipeline()
