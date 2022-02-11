import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events;"
staging_songs_table_drop = "drop table if exists stg_songs;"
songplay_table_drop = "drop table if exists songplays;"
staging_user_table_drop = "drop table if exists stg_users;"
staging_artist_table_drop = "drop table if exists stg_artists;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create = ("""
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
        songplay_id int identity(1, 1) primary key,
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

staging_user_table_create = ("""
    create table if not exists stg_users (
        user_id         int,
        first_name      varchar,
        last_name       varchar,
        gender          varchar,
        level           varchar,
        last_updated    bigint
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

staging_artist_table_create = ("""
    create table if not exists stg_artists (
        artist_id   varchar primary key,
        name        varchar sortkey,
        location    varchar,
        latitude    float,
        longitude   float,
        year        int
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

staging_users_copy = ("""
    insert into stg_users (
        select distinct e.userId, e.firstName, e.lastName, e.gender, null, 0
        from stg_events e
        where e.page = 'NextSong'
    );

    update stg_users
    set level = e.level, last_updated = e.ts
    from stg_events e
    where user_id = e.userId and e.page = 'NextSong'
    and e.ts > last_updated;
""")

staging_artists_copy = ("""
    insert into stg_artists (
        select distinct s.artist_id, null, null, 0, 0, 0
        from stg_songs s
        where s.artist_id is not null
    );

    update stg_artists
    set name = s.artist_name, location = s.artist_location, latitude = s.artist_latitude, longitude = s.artist_longitude
    from stg_songs s
    where stg_artists.artist_id = s.artist_id and s.year > stg_artists.year;
""")

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    select
        (TIMESTAMP 'epoch' + e.ts * INTERVAL '1 Second ') as start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    from stg_events e
    left join songs s on e.song = s.title
    left join artists a on e.artist = a.name
    where e.page = 'NextSong';
""")

user_table_insert = ("""
    insert into users (
        select user_id, first_name, last_name, gender, level
        from stg_users
    );
    drop table if exists stg_users;
""")

song_table_insert = ("""
    insert into songs (
        select distinct song_id, title, artist_id, year, duration
        from stg_songs
    );
""")

artist_table_insert = ("""
    insert into artists (
        select distinct artist_id, name, location, latitude, longitude
        from stg_artists
    );
    drop table if exists stg_artists;
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

create_table_queries = [staging_events_table_create, staging_songs_table_create, staging_user_table_create,
                        staging_artist_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, staging_user_table_drop,
                      staging_artist_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy,
                      staging_users_copy, staging_artists_copy]
insert_table_queries = [song_table_insert, artist_table_insert,
                        time_table_insert, user_table_insert, songplay_table_insert]
