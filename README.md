![](https://img.shields.io/badge/-Spotify-green?style=for-the-badge&logo=spotify&logoColor=white) ![](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) 

![DBeaver](https://img.shields.io/badge/DBeaver-2C8DB3?style=flat&logo=dbeaver&logoColor=white) ![](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white) 

![VSCODE](https://img.shields.io/badge/Visual%20Studio%20Code-007ACC?logo=visualstudiocode&logoColor=fff&style=plastic)

# SPOTIFY LISTENING ANALYSIS 2025

*This project is a custom Spotify listening analysis based on real Spotify streaming history data.*

🎯 The goal is to transform raw Spotify export files into meaningful insights by analyzing listening behavior, top artists, top tracks, listening periods, and usage trends through SQL and data visualization.

This project is inspired by the Spotify Wrapper project created by GitHub user **[JoseBlancoSiles](https://github.com/JoseBlancoSiles/spotify-wrapper2025)**, but it will be recreated using my brother's Spotify data as the primary dataset.


### 1: DOWNLOADING SPOTIFY DATA

The first step in this project was collecting the raw Spotify data by downloading the user's streaming history in **JSON format** directly from Spotify's privacy settings page.

<img width="2560" height="1032" alt="download data 2" src="https://github.com/user-attachments/assets/70df38d4-ec43-400c-b56f-f8337f5f0319" />


### 2: DATABASE SETUP

The next step was setting up the PostgreSQL environment and designing the database structure for analysis.

I used **DBeaver** as the SQL client to create the schemas and tables required for the project.

<img width="1549" height="672" alt="dbeaver" src="https://github.com/user-attachments/assets/9109e775-b605-48e6-b288-2361bef74412" />

At this stage, I wrote the **DDL** statements to create the database structure before loading the data into PostgreSQL:

```sql

/*	DDL	*/


-- SCHEMA CREATION

SELECT current_database();

CREATE SCHEMA RAW;	-- RAW schema (original data)

CREATE SCHEMA PRS;	-- PRS schema (filtered data)


-----------------
-- RAW DATA TABLE
-----------------
-- This table stores all raw Spotify events with detailed metadata

CREATE TABLE RAW.SPOTIFY_EVENTS (
    id SERIAL PRIMARY KEY,
    end_time TIMESTAMP NOT NULL,
    artist_name TEXT,
    track_name TEXT,
    ms_played INTEGER NOT NULL,
    album_name TEXT,
    context TEXT,
    platform TEXT,
    user_id TEXT,
    conn_country TEXT,
    ip_addr TEXT,
    spotify_track_uri TEXT,
    episode_name TEXT,
    episode_show_name TEXT,
    spotify_episode_uri TEXT,
    audiobook_title TEXT,
    audiobook_uri TEXT,
    audiobook_chapter_uri TEXT,
    audiobook_chapter_title TEXT,
    reason_start TEXT,
    reason_end TEXT,
    shuffle BOOLEAN,
    skipped BOOLEAN,
    offline BOOLEAN,
    offline_timestamp TEXT,
    incognito_mode BOOLEAN
);

SELECT * FROM raw.spotify_events; -- preview raw data table

-----------------------
-- PROCESSED DATA TABLE
-----------------------
-- This table stores only 2025 data and only relevant columns for analysis


CREATE TABLE PRS.SPOTIFY_EVENTS_2025 (
    id SERIAL PRIMARY KEY,
    end_time TIMESTAMP NOT NULL,
    artist_name TEXT,
    track_name TEXT,
    ms_played INTEGER
);

-- Insert 2025 data from RAW table
INSERT INTO PRS.SPOTIFY_EVENTS_2025 
(end_time, artist_name, track_name, ms_played)
SELECT 
    end_time,
    artist_name,
    track_name,
    ms_played
FROM RAW.SPOTIFY_EVENTS
WHERE end_time >= '2025-01-01'
  AND artist_name IS NOT NULL;

```

## 3: JSON TO POSTGRESQL 

In this step, I used Python to load all Spotify `.json` files into PostgreSQL.  

The script combines the files into one dataset and inserts the raw data into the `RAW.spotify_events` table.

![ezgif com-video-to-gif-converter](https://github.com/user-attachments/assets/3c78cd0f-ab12-4d9c-ae58-9e98f24c5108)

>[!IMPORTANT]
> Before running the script, install the required Python libraries:
>
> ```python
> G:/Python/python.exe -m pip install psycopg2-binary pandas python-dotenv
> ```
