import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Load Parameters
LOG_DATA = config.get("S3", "log_data")
LOG_PATH = config.get("S3", "log_jsonpath")
SONG_DATA = config.get("S3", "song_data")
IAM_ROLE = config.get("IAM_ROLE", "arn")
REGION = config.get("CLUSTER", "region")

# TABLE NAMES

staging_events_table = "staging_events"
staging_songs_table = "staging_songs"
songplay_table = "songplays"
user_table = "users"
song_table = "songs"
artist_table = "artists"
time_table = "time"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS %s" % staging_events_table
staging_songs_table_drop = "DROP TABLE IF EXISTS %s" % staging_songs_table
songplay_table_drop = "DROP TABLE IF EXISTS %s" % songplay_table
user_table_drop = "DROP TABLE IF EXISTS %s" % user_table
song_table_drop = "DROP TABLE IF EXISTS %s" % song_table
artist_table_drop = "DROP TABLE IF EXISTS %s" % artist_table
time_table_drop = "DROP TABLE IF EXISTS %s" % time_table

# CREATE STAGING TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artists             VARCHAR,
        auth                VARCHAR,
        first_name          VARCHAR,
        gender              VARCHAR,
        item_in_session     INTEGER,
        last_name           VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        session_id          INTEGER,
        songs               VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        user_agent          VARCHAR,
        user_id             INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users (
        user_id             INTEGER NOT NULL DISTKEY, 
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR,
        PRIMARY KEY(user_id)
    ); 
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
         song_id            VARCHAR NOT NULL,
         title              VARCHAR,
         artist_id          VARCHAR DISTKEY,
         artist_name        VARCHAR,
         year               INTEGER,
         duration           FLOAT,
         PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id        VARCHAR NOT NULL DISTKEY,
        artist_name      VARCHAR,
        location         VARCHAR,
        latitude         FLOAT,
        longitude        FLOAT,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP NOT NULL DISTKEY SORTKEY,
        hour                INTEGER,
        day                 INTEGER,
        week                INTEGER,
        month               INTEGER,
        year                INTEGER,
        weekday             VARCHAR(10),
        PRIMARY KEY (start_time)
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         INTEGER IDENTITY(0,1) SORTKEY,
        start_time          TIMESTAMP,
        user_id             VARCHAR,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        session_id          VARCHAR,
        location            VARCHAR,
        user_agent          VARCHAR,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF 
    REGION '{}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, REGION, LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF 
    REGION '{}'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE, REGION)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays
            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT se.ts,
               se.user_id,
               se.level,
               se.songs,
               se.artists,
               se.session_id,
               se.location,
               se.user_agent
        FROM staging_events se
        JOIN staging_songs ss
            ON (ss.title = se.songs
            AND ss.artist_name = se.artists AND ss.duration = se.length)
            AND se.page  =  'NextSong'
""")

user_table_insert = ("""
   INSERT INTO users
	(user_id, first_name, last_name, gender, level)
   SELECT DISTINCT user_id,
      first_name,
      last_name,
      gender,
      level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
   INSERT INTO songs
	(song_id, title, artist_id, artist_name, year, duration)
   SELECT DISTINCT song_id,
      title,
      artist_id,
      artist_name,
      year,
      duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
     (artist_id, artist_name, location, latitude, longitude)
    SELECT DISTINCT
       artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
     FROM staging_songs
     WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time
     (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# CHECK
songplay_table_count = ("""
    SELECT count(*) FROM songplays
""")

user_table_count = ("""
   SELECT count(*) FROM user
""")

song_table_count = ("""
   SELECT count(*) FROM songs
""")

artist_table_count = ("""
    SELECT count(*) FROM artists
""")

time_table_count = ("""
    SELECT count(*) FROM time
""")


# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, \
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, \
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]

# ETL
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, \
                        songplay_table_insert]

# CHECK
all_tables_count_rows = [user_table_count, song_table_count, artist_table_count, time_table_count, \
                         songplay_table_count]
