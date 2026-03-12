# Spotify-Data-Pipeline #
Automated ETL pipeline that extracts Spotify playlist data, transforms it with pandas and loads it into MySQL

**Project Overview**
This project presents a fully automated ETL (Extract, Transform, Load) data pipeline built entirely inside VS Code on Windows. The pipeline extracts Spotify-style music data, cleans and transforms it using pandas, and loads it into a structured MySQL database — running automatically every day via Windows Task Scheduler.

**Tech Stacks**
1. Language: Python 3.11 — Core engine powering the entire pipeline.
2. Data Source: Spotify-style sample data (tracks, albums, artists) — structured identically to the Spotify Web API response format.
3. API Library: Spotipy — Integrated and ready to switch to live Spotify API when Premium developer access is available
4. Data Processing: Pandas — Cleans, reshapes, and validates raw JSON into structured DataFrames.
5. Database: MySQL 8.0 — Stores all transformed data in a star schema (artists → albums → tracks).
6. DB Connector: mysql-connector-python — Loads data into MySQL with upsert (no duplicates ever).
7. Scheduling: Windows Task Scheduler — Runs pipeline automatically every day via run_pipeline.bat.
8. Environment: python-dotenv — Loads API keys and passwords securely from .env file.
9. IDE: VS Code — Built entirely using VS Code terminal, editor, and MySQL extension.
