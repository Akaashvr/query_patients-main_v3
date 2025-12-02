import os
import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time

from utils import get_db_url


STAGING_CREATE_SQL = """
-- Drop existing tables if they exist (facts -> cores -> dimensions -> staging)
DROP TABLE IF EXISTS user_anime_ratings CASCADE;
DROP TABLE IF EXISTS anime_genres CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS anime CASCADE;

DROP TABLE IF EXISTS anime_types CASCADE;
DROP TABLE IF EXISTS anime_statuses CASCADE;
DROP TABLE IF EXISTS studios CASCADE;
DROP TABLE IF EXISTS sources CASCADE;
DROP TABLE IF EXISTS rating_categories CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS age_groups CASCADE;
DROP TABLE IF EXISTS genders CASCADE;
DROP TABLE IF EXISTS watch_statuses CASCADE;

DROP TABLE IF EXISTS stage_anime CASCADE;
DROP TABLE IF EXISTS stage_genres CASCADE;
DROP TABLE IF EXISTS stage_users CASCADE;
DROP TABLE IF EXISTS stage_ratings CASCADE;

-- =========================
-- Staging tables
-- =========================

CREATE TABLE stage_anime (
    AnimeID        TEXT,
    AnimeTitle     TEXT,
    Type           TEXT,
    Episodes       TEXT,
    Status         TEXT,
    StartDate      TIMESTAMP,
    EndDate        TIMESTAMP,
    Source         TEXT,
    StudioName     TEXT,
    RatingCategory TEXT,
    OverallScore   TEXT,
    PopularityRank TEXT
);

CREATE TABLE stage_genres (
    AnimeID    TEXT,
    GenreName  TEXT
);

CREATE TABLE stage_users (
    UserID    TEXT,
    UserName  TEXT,
    Country   TEXT,
    AgeGroup  TEXT,
    Gender    TEXT
);

CREATE TABLE stage_ratings (
    UserID      TEXT,
    AnimeID     TEXT,
    UserScore   TEXT,
    RatingDate  TIMESTAMP,
    WatchStatus TEXT
);

-- =========================
-- Dimension / lookup tables
-- =========================

CREATE TABLE anime_types (
    type_id   SERIAL PRIMARY KEY,
    type_name TEXT NOT NULL UNIQUE
);

CREATE TABLE anime_statuses (
    status_id   SERIAL PRIMARY KEY,
    status_desc TEXT NOT NULL UNIQUE
);

CREATE TABLE studios (
    studio_id   SERIAL PRIMARY KEY,
    studio_name TEXT NOT NULL UNIQUE
);

CREATE TABLE sources (
    source_id   SERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
);

CREATE TABLE rating_categories (
    rating_category_id SERIAL PRIMARY KEY,
    rating_code        TEXT NOT NULL UNIQUE
);

CREATE TABLE genres (
    genre_id   SERIAL PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

CREATE TABLE countries (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE age_groups (
    age_group_id    SERIAL PRIMARY KEY,
    age_group_label TEXT NOT NULL UNIQUE
);

CREATE TABLE genders (
    gender_id   SERIAL PRIMARY KEY,
    gender_desc TEXT NOT NULL UNIQUE
);

CREATE TABLE watch_statuses (
    watch_status_id SERIAL PRIMARY KEY,
    status_desc     TEXT NOT NULL UNIQUE
);

-- =========================
-- Core entity tables
-- =========================

CREATE TABLE anime (
    anime_id            TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    type_id             INTEGER,
    status_id           INTEGER,
    episodes            INTEGER,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    source_id           INTEGER,
    studio_id           INTEGER,
    rating_category_id  INTEGER,
    overall_score       REAL,
    popularity_rank     INTEGER,
    FOREIGN KEY (type_id)            REFERENCES anime_types(type_id),
    FOREIGN KEY (status_id)          REFERENCES anime_statuses(status_id),
    FOREIGN KEY (source_id)          REFERENCES sources(source_id),
    FOREIGN KEY (studio_id)          REFERENCES studios(studio_id),
    FOREIGN KEY (rating_category_id) REFERENCES rating_categories(rating_category_id)
);

CREATE TABLE users (
    user_id      TEXT PRIMARY KEY,
    user_name    TEXT NOT NULL,
    country_id   INTEGER,
    age_group_id INTEGER,
    gender_id    INTEGER,
    FOREIGN KEY (country_id)   REFERENCES countries(country_id),
    FOREIGN KEY (age_group_id) REFERENCES age_groups(age_group_id),
    FOREIGN KEY (gender_id)    REFERENCES genders(gender_id)
);

-- =========================
-- Fact tables
-- =========================

CREATE TABLE anime_genres (
    anime_id TEXT NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES anime(anime_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE user_anime_ratings (
    user_id        TEXT NOT NULL,
    anime_id       TEXT NOT NULL,
    user_score     REAL,
    rating_date    TIMESTAMP,
    watch_status_id INTEGER,
    PRIMARY KEY (user_id, anime_id),
    FOREIGN KEY (user_id)        REFERENCES users(user_id),
    FOREIGN KEY (anime_id)       REFERENCES anime(anime_id),
    FOREIGN KEY (watch_status_id) REFERENCES watch_statuses(watch_status_id)
);
"""

FILES = {
    "anime": {
        "filename": "AnimeCorePopulatedTable.txt",
    },
    "genres": {
        "filename": "AnimeGenresCorePopulatedTable.txt",
    },
    "users": {
        "filename": "UsersCorePopulatedTable.txt",
    },
    "ratings": {
        "filename": "AnimeUserRatingsCorePopulatedTable.txt",
        "batch_size": 100_000,
    },
}

EXPECTED_COLUMNS = {
    "anime": [
        "AnimeID",
        "AnimeTitle",
        "Type",
        "Episodes",
        "Status",
        "StartDate",
        "EndDate",
        "Source",
        "StudioName",
        "RatingCategory",
        "OverallScore",
        "PopularityRank",
    ],
    "genres": [
        "AnimeID",
        "GenreName",
    ],
    "users": [
        "UserID",
        "UserName",
        "Country",
        "AgeGroup",
        "Gender",
    ],
    "ratings": [
        "UserID",
        "AnimeID",
        "UserScore",
        "RatingDate",
        "WatchStatus",
    ],
}


def load_tsv_to_stage(conn, filepath, stage_table, expected_columns, batch_size=5_000):
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {filepath}")

    with path.open("r", encoding="utf-8-sig") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter="\t")

        # validate columns
        missing = sorted(set(expected_columns) - set(csv_reader.fieldnames))
        if missing:
            raise ValueError(f"{filepath} missing expected columns: {missing}")

        placeholders = ", ".join(["%s"] * len(expected_columns))
        sql = f"INSERT INTO {stage_table} ({', '.join(expected_columns)}) VALUES ({placeholders})"
        rows = []
        row_count = 0
        total_count = 0
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM {stage_table}")
        conn.commit()
        print(f"Cleaned up rows from {stage_table}")

        log_template = "Inserted another batch of {:,} rows; total: {:,}"

        for row in csv_reader:
            rows.append([row.get(c, None) for c in expected_columns])
            row_count += 1

            if row_count == batch_size:
                extras.execute_batch(cursor, sql, rows)
                conn.commit()
                total_count += len(rows)
                row_count = 0
                rows = []
                print(log_template.format(batch_size, total_count))

        if rows:
            extras.execute_batch(cursor, sql, rows)
            conn.commit()
            total_count += len(rows)
            print(log_template.format(len(rows), total_count))

        cursor.close()
        print(f"Finished loading data into {stage_table}")


def build_dimensions(conn):
    cur = conn.cursor()

    # Anime types (TV, Movie, OVA, etc.)
    cur.execute(
        """
        INSERT INTO anime_types(type_name)
        SELECT DISTINCT Type
        FROM stage_anime
        WHERE Type IS NOT NULL AND Type <> ''
        ON CONFLICT (type_name) DO NOTHING;
        """
    )

    # Anime statuses (Finished, Ongoing, etc.)
    cur.execute(
        """
        INSERT INTO anime_statuses(status_desc)
        SELECT DISTINCT Status
        FROM stage_anime
        WHERE Status IS NOT NULL AND Status <> ''
        ON CONFLICT (status_desc) DO NOTHING;
        """
    )

    # Studios
    cur.execute(
        """
        INSERT INTO studios(studio_name)
        SELECT DISTINCT StudioName
        FROM stage_anime
        WHERE StudioName IS NOT NULL AND StudioName <> ''
        ON CONFLICT (studio_name) DO NOTHING;
        """
    )

    # Sources (Manga, Light Novel, Original, etc.)
    cur.execute(
        """
        INSERT INTO sources(source_name)
        SELECT DISTINCT Source
        FROM stage_anime
        WHERE Source IS NOT NULL AND Source <> ''
        ON CONFLICT (source_name) DO NOTHING;
        """
    )

    # Rating categories (G, PG-13, R, etc.)
    cur.execute(
        """
        INSERT INTO rating_categories(rating_code)
        SELECT DISTINCT RatingCategory
        FROM stage_anime
        WHERE RatingCategory IS NOT NULL AND RatingCategory <> ''
        ON CONFLICT (rating_code) DO NOTHING;
        """
    )

    # Genres
    cur.execute(
        """
        INSERT INTO genres(genre_name)
        SELECT DISTINCT GenreName
        FROM stage_genres
        WHERE GenreName IS NOT NULL AND GenreName <> ''
        ON CONFLICT (genre_name) DO NOTHING;
        """
    )

    # Countries
    cur.execute(
        """
        INSERT INTO countries(country_name)
        SELECT DISTINCT Country
        FROM stage_users
        WHERE Country IS NOT NULL AND Country <> ''
        ON CONFLICT (country_name) DO NOTHING;
        """
    )

    # Age groups
    cur.execute(
        """
        INSERT INTO age_groups(age_group_label)
        SELECT DISTINCT AgeGroup
        FROM stage_users
        WHERE AgeGroup IS NOT NULL AND AgeGroup <> ''
        ON CONFLICT (age_group_label) DO NOTHING;
        """
    )

    # Genders
    cur.execute(
        """
        INSERT INTO genders(gender_desc)
        SELECT DISTINCT Gender
        FROM stage_users
        WHERE Gender IS NOT NULL AND Gender <> ''
        ON CONFLICT (gender_desc) DO NOTHING;
        """
    )

    # Watch statuses (Completed, Watching, Plan to Watch, etc.)
    cur.execute(
        """
        INSERT INTO watch_statuses(status_desc)
        SELECT DISTINCT WatchStatus
        FROM stage_ratings
        WHERE WatchStatus IS NOT NULL AND WatchStatus <> ''
        ON CONFLICT (status_desc) DO NOTHING;
        """
    )

    conn.commit()
    cur.close()
    print("Dimension tables populated")


def load_entities(conn):
    cur = conn.cursor()

    # Anime
    cur.execute(
        """
        INSERT INTO anime (
            anime_id,
            title,
            type_id,
            status_id,
            episodes,
            start_date,
            end_date,
            source_id,
            studio_id,
            rating_category_id,
            overall_score,
            popularity_rank
        )
        SELECT
            s.AnimeID,
            s.AnimeTitle,
            t.type_id,
            st.status_id,
            NULLIF(s.Episodes, '')::INTEGER,
            s.StartDate,
            s.EndDate,
            src.source_id,
            stu.studio_id,
            rc.rating_category_id,
            NULLIF(s.OverallScore, '')::REAL,
            NULLIF(s.PopularityRank, '')::INTEGER
        FROM stage_anime s
        LEFT JOIN anime_types t         ON t.type_name = s.Type
        LEFT JOIN anime_statuses st     ON st.status_desc = s.Status
        LEFT JOIN sources src           ON src.source_name = s.Source
        LEFT JOIN studios stu           ON stu.studio_name = s.StudioName
        LEFT JOIN rating_categories rc  ON rc.rating_code = s.RatingCategory
        ON CONFLICT (anime_id) DO NOTHING;
        """
    )

    # Users
    cur.execute(
        """
        INSERT INTO users (
            user_id,
            user_name,
            country_id,
            age_group_id,
            gender_id
        )
        SELECT
            s.UserID,
            s.UserName,
            c.country_id,
            ag.age_group_id,
            g.gender_id
        FROM stage_users s
        LEFT JOIN countries c  ON c.country_name = s.Country
        LEFT JOIN age_groups ag ON ag.age_group_label = s.AgeGroup
        LEFT JOIN genders g    ON g.gender_desc = s.Gender
        ON CONFLICT (user_id) DO NOTHING;
        """
    )

    conn.commit()
    cur.close()
    print("Entity tables populated")


def build_facts(conn):
    cur = conn.cursor()

    # Anime genres
    cur.execute(
        """
        INSERT INTO anime_genres (anime_id, genre_id)
        SELECT
            s.AnimeID,
            g.genre_id
        FROM stage_genres s
        JOIN genres g ON g.genre_name = s.GenreName
        JOIN anime a ON a.anime_id = s.AnimeID
        ON CONFLICT (anime_id, genre_id) DO NOTHING;
        """
    )

    # User ratings
    cur.execute(
        """
        INSERT INTO user_anime_ratings (
            user_id,
            anime_id,
            user_score,
            rating_date,
            watch_status_id
        )
        SELECT
            s.UserID,
            s.AnimeID,
            NULLIF(s.UserScore, '')::REAL,
            s.RatingDate,
            ws.watch_status_id
        FROM stage_ratings s
        JOIN watch_statuses ws ON ws.status_desc = s.WatchStatus
        JOIN users u           ON u.user_id = s.UserID
        JOIN anime a           ON a.anime_id = s.AnimeID
        ON CONFLICT (user_id, anime_id) DO NOTHING;
        """
    )

    conn.commit()
    cur.close()
    print("Fact tables populated")


# Main execution
if __name__ == "__main__":
    DATABASE_URL = get_db_url()

    # Create tables
    print("Creating tables...")
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected to DB")
    cursor = conn.cursor()
    print("About to execute STAGING_CREATE_SQL")
    cursor.execute(STAGING_CREATE_SQL)
    print("Finished executing STAGING_CREATE_SQL, about to commit")
    conn.commit()
    print("Commit done")
    cursor.close()
    conn.close()
    print("Tables created successfully\n")

    # Load staging data
    print("Loading staging data...")
    start_time = time.monotonic()
    conn = psycopg2.connect(DATABASE_URL)

    for name in FILES:
        load_tsv_to_stage(
            conn,
            FILES[name]["filename"],
            f"stage_{name}",
            EXPECTED_COLUMNS[name],
            FILES[name].get("batch_size", 5_000),
        )

    conn.close()
    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    print(f"\nStaging data loaded. Elapsed time: {elapsed_time:.2f} seconds\n")

    # Build dimensions
    print("Building dimension tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_dimensions(conn)
    conn.close()

    # Load entities
    print("Loading entity tables...")
    conn = psycopg2.connect(DATABASE_URL)
    load_entities(conn)
    conn.close()

    # Build facts
    print("Building fact tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_facts(conn)
    conn.close()

    print("\n✅ Anime database migration complete!")
