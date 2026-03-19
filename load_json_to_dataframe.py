
import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_values
import glob
import os


def load_json_to_dataframe(file_path):
    """
    Load JSON data from a file into a pandas DataFrame.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        df = pd.DataFrame(data)
        print(f"Loaded: {file_path} | Rows: {len(df)}")
        return df

    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        return None


def dump_data_to_postgres(df, connection_params):
    """
    Insert Spotify streaming history data into RAW.spotify_events.
    """
    conn = None
    cursor = None

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Insert query
        insert_query = """
        INSERT INTO RAW.spotify_events (
            end_time,
            artist_name,
            track_name,
            ms_played,
            album_name,
            context,
            platform,
            user_id,
            conn_country,
            ip_addr,
            spotify_track_uri,
            episode_name,
            episode_show_name,
            spotify_episode_uri,
            audiobook_title,
            audiobook_uri,
            audiobook_chapter_uri,
            audiobook_chapter_title,
            reason_start,
            reason_end,
            shuffle,
            skipped,
            offline,
            offline_timestamp,
            incognito_mode
        ) VALUES %s
        """

        # Prepare data for insertion
        values = [
            (
                row.get("ts"),  # maps to end_time
                row.get("master_metadata_album_artist_name"),
                row.get("master_metadata_track_name"),
                row.get("ms_played"),
                row.get("master_metadata_album_album_name"),
                None,  # context not available in current JSON structure
                row.get("platform"),
                row.get("username"),  # maps to user_id
                row.get("conn_country"),
                row.get("ip_addr"),
                row.get("spotify_track_uri"),
                row.get("episode_name"),
                row.get("episode_show_name"),
                row.get("spotify_episode_uri"),
                row.get("audiobook_title"),
                row.get("audiobook_uri"),
                row.get("audiobook_chapter_uri"),
                row.get("audiobook_chapter_title"),
                row.get("reason_start"),
                row.get("reason_end"),
                row.get("shuffle"),
                row.get("skipped"),
                row.get("offline"),
                str(row.get("offline_timestamp")) if row.get("offline_timestamp") is not None else None,
                row.get("incognito_mode")
            )
            for _, row in df.iterrows()
        ]

        # Batch insert
        execute_values(cursor, insert_query, values)
        conn.commit()

        print(f"\nSuccessfully inserted {len(values)} rows into RAW.spotify_events.")

    except Exception as e:
        print(f"\nError inserting data into PostgreSQL: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    # Folder containing all JSON files
    folder_path = "Spotify Extended Streaming History"

    # Get all JSON files from the folder
    json_files = glob.glob(os.path.join(folder_path, "*.json"))

    # Optional: exclude video history file if you only want audio files
    json_files = [file for file in json_files if "Video" not in os.path.basename(file)]

    if not json_files:
        print("No JSON files found in the folder.")
        exit()

    print("JSON files found:")
    for file in json_files:
        print(f" - {file}")

    # Load all JSON files into DataFrames
    dataframes = []

    for file_path in json_files:
        df = load_json_to_dataframe(file_path)
        if df is not None:
            dataframes.append(df)

    if dataframes:
        # Combine all DataFrames into one
        combined_df = pd.concat(dataframes, ignore_index=True)

        print("\nAll JSON files loaded successfully!")
        print(f"Total rows combined: {len(combined_df)}")
        print("\nPreview:")
        print(combined_df.head())

        # PostgreSQL connection parameters
        connection_params = {
            "dbname": "postgres",      # change if your DB name is different
            "user": "postgres",        # change if your PostgreSQL user is different
            "password": "postgres",   # <<< CHANGE THIS
            "host": "localhost",
            "port": "5432"
        }

        # Insert data into PostgreSQL
        dump_data_to_postgres(combined_df, connection_params)

    else:
        print("No JSON files were loaded.")
