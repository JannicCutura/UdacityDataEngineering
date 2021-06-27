import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg') # the config file is not tracked by git to not accidentally spoil my credentials

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"



# Staging Events Table
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

# Copy to staging events table
staging_events_copy = f"""
    COPY staging_events_table FROM {LOG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    COMPUPDATE OFF region 'eu-central-1'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {LOG_PATH};
"""


# Staging Songs Table
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    song_id            VARCHAR,
    num_songs          INTEGER,
    title              VARCHAR,
    artist_name        VARCHAR,
    artist_latitude    FLOAT,
    year               INTEGER,
    duration           FLOAT,
    artist_id          VARCHAR,
    artist_longitude   FLOAT,
    artist_location    VARCHAR);
""")

# Copy to Songs table
staging_songs_copy = f"""
    COPY staging_songs_table FROM {SONG_DATA}
    CREDENTIALS 'aws_iam_role={IAM_ROLE}'
    COMPUPDATE OFF region 'eu-central-1'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""



## 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
    start_time           TIMESTAMP,
    user_id              INTEGER,
    level                VARCHAR,
    song_id              VARCHAR,
    artist_id            VARCHAR,
    session_id           INTEGER,
    location             VARCHAR,
    user_agent           VARCHAR);
""")


songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_events_table se
JOIN staging_songs_table ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")



user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id INTEGER PRIMARY KEY distkey,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR);
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events_table
where userId IS NOT NULL;
""")




song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR distkey,
    year        INTEGER,
    duration    FLOAT);
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs_table
WHERE song_id IS NOT NULL;
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id          VARCHAR PRIMARY KEY distkey,
    name               VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT);
""")


artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs_table
where artist_id IS NOT NULL;
""")



time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
    hour          INTEGER,
    day           INTEGER,
    week          INTEGER,
    month         INTEGER,
    year          INTEGER,
    weekday       INTEGER);
""")

time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_events_table
WHERE ts IS NOT NULL;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]




