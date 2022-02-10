import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events;"
staging_songs_table_drop = "drop table if exists stg_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists stg_events (
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
        registration float,
        sessionId int,
        song varchar,
        status varchar,
        ts bigint,
        userAgent varchar,
        userId int
    );
""")

staging_songs_table_create = ("""
    create table if not exists stg_songs (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
    );
""")

songplay_table_create = ("""
    create table if not exists songplays (
        songplay_id serial primary key,
        start_time  timestamp,
        user_id     int,
        level       varchar,
        song_id     varchar distkey,
        artist_id   varchar,
        session_id  int,
        location    varchar,
        user_agent  varchar,
        foreign key (user_id) references users(user_id),
        foreign key (start_time) references time(start_time)
    );
""")

user_table_create = ("""
    create table if not exists users (
        user_id     int primary key distkey,
        first_name  varchar sortkey,
        last_name   varchar,
        gender      varchar,
        level       varchar
    );
""")

song_table_create = ("""
    create table if not exists songs (
        song_id     varchar primary key distkey,
        title       varchar sortkey,
        artist_id   varchar,
        year        int,
        duration    float
    );
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id   varchar primary key,
        name        varchar sortkey,
        location    varchar,
        latitude    float,
        longitude   float
    ) diststyle all;
""")

time_table_create = ("""
    create table if not exists time (
        start_time  timestamp primary key sortkey,
        hour        int,
        day         int,
        week        int,
        month       int,
        year        int,
        weekday     int
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    format as json 'auto ignorecase'
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    copy stg_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    format as json 'auto ignorecase'
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
    insert into users (
        select userId, firstName, lastName, gender, level
        from stg_events
        where page = 'NextSong'
    );
""")

song_table_insert = ("""
    insert into songs (
        select distinct song_id, title, artist_id, year, duration
        from stg_songs
    );
""")

artist_table_insert = ("""
    insert into artists (
        select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        from stg_songs
    );
""")

time_table_insert = ("""
    insert into time (
        select
            distinct (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second '),
            extract(hour from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')),
            extract(day from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')),
            extract(week from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')),
            extract(month from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')),
            extract(year from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second ')),
            extract(weekday from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second '))
        from stg_events
        where page = 'NextSong'
    );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
