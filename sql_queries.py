import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    itemInSession int,
                                    lastName varchar,
                                    length float,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration bigint,
                                    sessionId int,
                                    song varchar,
                                    status varchar,
                                    ts bigint,
                                    userAgent varchar(max),
                                    userId int
                                    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs int,
                                artist_id varchar,
                                artist_latitude float,
                                artist_longitude float,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration float,
                                year int);
""")

# songplay_id as PRIMARY KEY
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int IDENTITY(0,1), 
                                start_time timestamp NOT NULL, 
                                user_id varchar NOT NULL, 
                                level varchar, 
                                song_id varchar, 
                                artist_id varchar, 
                                session_id int, 
                                location varchar, 
                                user_agent varchar,
                                PRIMARY KEY (songplay_id),
                                FOREIGN KEY(start_time) REFERENCES time (start_time),
                                FOREIGN KEY(user_id) REFERENCES users (user_id),
                                FOREIGN KEY(song_id) REFERENCES songs (song_id),        
                                FOREIGN KEY(artist_id) REFERENCES artists (artist_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id varchar PRIMARY KEY, 
                            first_name varchar, 
                            last_name varchar, 
                            gender varchar, 
                            level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar PRIMARY KEY,
                            title varchar,
                            artist_id varchar, 
                            year int, 
                            duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                                artist_id varchar PRIMARY KEY, 
                                name varchar, 
                                location varchar, 
                                latitude real, 
                                longitude real);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time timestamp PRIMARY KEY, 
                            hour int NOT NULL, 
                            day int NOT NULL, 
                            week int NOT NULL, 
                            month int NOT NULL, 
                            year int NOT NULL, 
                            weekday int NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    json {}
    region 'us-west-2';
    """).format(config.get('S3','LOG_DATA'),config.get("IAM_ROLE","ARN"),config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    json 'auto'
    region 'us-west-2';
    """).format(config.get('S3','SONG_DATA'),config.get("IAM_ROLE","ARN"))

# FINAL TABLES
# Time conversion borrowed from, https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""
        INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)   
        SELECT 
            timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
            e.userId as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent       
        FROM staging_events e
        JOIN staging_songs s ON (e.artist=s.artist_name AND e.length=s.duration AND e.song=s.title)
        WHERE e.page = 'NextSong';
        
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT 
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events 
    WHERE page = 'NextSong';
""")

# Distinct to only insert a song once.
song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title,
        artist_id,
        year,
        duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs;
""")
# Distinct to insert an artist only once
artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT 
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM 
        staging_songs;
""")

# Extract parts from date - https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        s.start_time,
        extract(hour from s.start_time),
        extract(day from s.start_time),
        extract(week from s.start_time),
        extract(month from s.start_time),
        extract(year from s.start_time),
        extract(weekday from s.start_time)
    FROM songplays s;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
