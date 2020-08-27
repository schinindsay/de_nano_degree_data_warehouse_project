import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE "staging_events" (
    "id" int IDENTITY(1,1) NOT NULL,
    "artist" character varying,
    "auth" character varying,
    "firstName" character varying,
    "gender" character varying(1),
    "itemSession" int,
    "lastName" character varying,
    "length" double precision,
    "level" character varying(4),
    "location" character varying,
    "method" character varying,
    "page" character varying,
    "registration" bigint,
    "sessionId" int,
    "song" character varying,
    "status" int,
    "ts" bigint,
    "userAgent" character varying,
    "userId" int)
;
""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs" (
    "id" int IDENTITY(1,1) NOT NULL,
    "num_songs" int, 
    "artist_id" character varying(18), 
    "artist_latitude" double precision, 
    "artist_longitude" double precision,
    "artist_location" character varying, 
    "artist_name" character varying, 
    "song_id" character varying(18), 
    "title" character varying, 
    "duration" double precision,
    "year" int)
;
""")


songplay_table_create = ("""
CREATE TABLE "songplays" (
    "songplay_id" int IDENTITY(0,1) PRIMARY KEY, 
    "start_time" timestamp NOT NULL, 
    "user_id" int NOT NULL, 
    "level" character varying(4) NOT NULL, 
    "song_id" character varying(18) NOT NULL, 
    "artist_id" character varying(18) NOT NULL, 
    "session_id" int NOT NULL, 
    "location" character varying, 
    "user_agent" character varying)
;
""")

user_table_create = ("""
CREATE TABLE "users" (
    "user_id" int PRIMARY KEY, 
    "first_name" character varying NOT NULL, 
    "last_name" character varying NOT NULL, 
    "gender" character varying(1), 
    "level" character varying(4) NOT NULL)
;
""")

song_table_create = ("""
CREATE TABLE "songs" (
    "song_id" character varying(18) NOT NULL, 
    "title" character varying NOT NULL, 
    "artist_id" character varying(18) NOT NULL, 
    "year" int, 
    "duration" double precision)
;
""")

artist_table_create = ("""
CREATE TABLE "artists" (
    "artist_id" character varying(18) NOT NULL, 
    "name" character varying NOT NULL, 
    "location" character varying, 
    "lattitude" double precision, 
    "longitude" double precision)
;
""")

time_table_create = ("""
CREATE TABLE "time" (
    "start_time" timestamp NOT NULL PRIMARY KEY,
    "hour" int,
    "day" int,
    "week" int,
    "month" int,
    "year" int,
    "weekday" int)
;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    region 'us-west-2'
;
""").format(ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data/A/A'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    region 'us-west-2'
;
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s on e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
    AND e.userId IS NOT NULL
    AND s.song_id IS NOT NULL
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE userId IS NOT NULL
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
    FROM songplays
    WHERE start_time IS NOT NULL
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
