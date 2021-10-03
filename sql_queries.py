import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """
    CREATE TABLE IF NOT EXISTS
      staging_events (
        artist VARCHAR,
        auth VARCHAR(20),
        first_name VARCHAR(30),
        gender VARCHAR(1),
        item_in_session INT,
        last_name VARCHAR(30),
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR(3),
        page VARCHAR(20),
        registration DECIMAL,
        session_id INT,
        song VARCHAR,
        status SMALLINT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT
      )
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS
      staging_songs (
        num_songs SMALLINT,
        artist_id VARCHAR(25),
        artist_longitude FLOAT,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR(25),
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
      )
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS
      songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INT NOT NULL REFERENCES users(user_id),
        level VARCHAR(4),
        song_id VARCHAR(25) NOT NULL REFERENCES songs(song_id),
        artist_id VARCHAR(25) NOT NULL REFERENCES artists(artist_id),
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
      )
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS
      users (
        user_id INT PRIMARY KEY DISTKEY,
        first_name VARCHAR(30),
        last_name VARCHAR(30) SORTKEY,
        gender VARCHAR(1),
        level VARCHAR(4)
      )
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS
      songs (
        song_id VARCHAR(25) PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR(25) NOT NULL DISTKEY REFERENCES artists(artist_id),
        year SMALLINT,
        duration FLOAT
      )
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS
      artists (
        artist_id VARCHAR(25) PRIMARY KEY DISTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
      )
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS
      time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week_of_year INT,
        month INT,
        year INT,
        weekday BOOLEAN
      )
"""

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO
      songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
      )
    SELECT
      TIMESTAMP 'epoch' + e.timestamp * INTERVAL '1 second' as start_time,
      e.user_id,
      e.level,
      s.song_id,
      s.artist_id,
      e.session_id,
      e.location,
      e.user_agent
    FROM
      staging_events e
    JOIN
      staging_songs s
    ON
      e.artist = s.artist_name AND
      e.length = s.duration AND
      e.song = s.title
    WHERE
      e.page = 'NextSong'
"""

user_table_insert = """
    INSERT INTO
      users (
        user_id,
        first_name,
        last_name,
        gender,
        level
      )
    SELECT
      DISTINCT user_id,
      first_name,
      last_name,
      gender,
      level
    FROM
      staging_events
    WHERE
      page = 'NextSong'
"""

song_table_insert = """
    INSERT INTO
      songs (
        song_id,
        title,
        artist_id,
        year,
        duration
      )
    SELECT
      DISTINCT song_id,
      title,
      artist_id,
      year,
      duration
    FROM
      staging_songs
"""

artist_table_insert = """
    INSERT INTO
      artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
      )
    SELECT
      DISTINCT artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude
    FROM
      staging_songs
"""

time_table_insert = """
    INSERT INTO
      time (
        start_time,
        hour,
        day,
        week_of_year,
        month,
        year,
        weekday
      )
    SELECT
      start_time,
      EXTRACT(HOUR FROM start_time) AS hour,
      EXTRACT(DAY FROM start_time) AS day,
      EXTRACT(WEEK FROM start_time) AS week,
      EXTRACT(MONTH FROM start_time) AS month,
      EXTRACT(YEAR FROM start_time) AS year,
      CASE WHEN EXTRACT(DAY FROM start_time) IN (6, 7) THEN false ELSE true
        END AS weekday
    FROM (
      SELECT
        TIMESTAMP 'epoch' + timestamp * INTERVAL '1 second' as start_time
      FROM
        staging_events
      ) s
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    artist_table_create,
    user_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]