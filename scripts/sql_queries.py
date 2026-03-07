import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists fact_songplays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artists"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists staging_events
                              (artist varchar,
                               auth varchar,
                               first_name varchar,
                               gender varchar,
                               item_in_session int,
                               last_name varchar,
                               length float,
                               level varchar,
                               location varchar,
                               method varchar,
                               page varchar,
                               registration float,
                               session_id int,
                               song varchar,
                               status int,
                               ts bigint,
                               user_agent varchar,
                               user_id int
                              )
""")

staging_songs_table_create = ("""create table if not exists staging_songs
                              (num_songs int,
                               artist_id varchar,
                               artist_latitude float,
                               artist_longitude float,
                               artist_location varchar,
                               artist_name varchar,
                               song_id varchar,
                               title varchar,
                               duration float,
                               year int
                              )
""")

songplay_table_create = ("""create table if not exists fact_songplays
                         (songplay_id int identity(0,1) primary key,
                          song_id varchar,
                          user_id int,
                          level varchar,
                          start_time timestamp,
                          artist_id varchar,
                          session_id int,
                          location varchar,
                          user_agent varchar
                         )
""")

user_table_create = ("""create table if not exists dim_users
                     (user_id int primary key,
                      first_name varchar,
                      last_name varchar,
                      gender varchar,
                      level varchar
                     )
""")

song_table_create = ("""create table if not exists dim_songs
                     (song_id varchar primary key,
                      title varchar,
                      artist_id varchar,
                      year int,
                      duration float
                     )
""")

artist_table_create = ("""create table if not exists dim_artists
                       (artist_id varchar primary key,
                        name varchar,
                        location varchar,
                        latitude float,
                        longitude float
                       )
""")


time_table_create = ("""create table if not exists dim_time
                     (start_time timestamp primary key,
                      hour int,
                      day int, 
                      week int,
                      month int,
                      year int,
                      weekday int
                     )
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events
                        from 's3://udacity-dend/log_data'
                        IAM_ROLE '{}'
                        format as json 's3://udacity-dend/log_json_path.json'
                        region 'us-west-2'
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
                      copy staging_songs
                      from 's3://udacity-dend/song_data'
                      iam_role '{}'
                      format as json 'auto'
                      region 'us-west-2'
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""insert into fact_songplays
                         (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                         select distinct
                         TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
                         e.user_id,
                         e.level,
                         s.song_id,
                         s.artist_id,
                         e.session_id,
                         e.location,
                         e.user_agent
                         from staging_events e
                         left join staging_songs s
                         on e.song = s.title 
                         and e.artist = s.artist_name
                         where e.page = 'NextSong'
""")

user_table_insert = ("""insert into dim_users(user_id, first_name, last_name, gender, level)
                     select distinct
                     user_id,
                     first_name,
                     last_name,
                     gender,
                     level
                     from staging_events
                     where user_id is not null
                     and page = 'NextSong'
""")

song_table_insert = ("""insert into dim_songs(song_id, title, artist_id, year, duration)
                     select distinct
                     song_id,
                     title,
                     artist_id,
                     year,
                     duration
                     from staging_songs
""")

artist_table_insert = ("""insert into dim_artists(artist_id, name, location, latitude, longitude)
                       select distinct
                       artist_id,
                       artist_name as name,
                       artist_location as location,
                       artist_latitude as latitude,
                       artist_longitude as longitude
                       from staging_songs
""")

time_table_insert = ("""insert into dim_time(start_time, hour, day, week, month, year, weekday)
                     select distinct
                     timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
                     extract(hour from timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
                     extract(day from timestamp 'epoch' + ts/1000 * interval '1 second') as day,
                     extract(week from timestamp 'epoch' + ts/1000 * interval '1 second') as week,
                     extract(month from timestamp 'epoch' + ts/1000 * interval '1 second') as month,
                     extract(year from timestamp 'epoch' + ts/1000 * interval '1 second') as year,
                     extract(dow from timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
                     from staging_events
                     where page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



# --------------------------------
# Analytical Queries
# --------------------------------


#How many song plays exist in the system?
total_songplays_query = ("""
                          select count(*) as total_songplays 
                          from fact_songplays
""")

# What are the top 10 most played songs?
top_songs_query = ("""
                    select top 10 s.title, count(*) as play_count from fact_songplays f
                    inner join dim_songs s
                    on f.song_id = s.song_id
                    group by s.title
                    order by play_count desc
""")

# Which users listen to the most songs?
top_users_query = ("""
                    select u.user_id, u.first_name, u.last_name, count(*) as play_count
                    from fact_songplays f
                    inner join dim_users u
                    on f.user_id = u.user_id
                    group by u.user_id, u.first_name, u.last_name
                    order by play_count desc
""")

# Which artists are played the most?
top_artists_query = ("""
                      select a.name as artist_name, count(*) as play_count
                      from fact_songplays f
                      inner join dim_artists a
                      on f.artist_id = a.artist_id
                      group by a.name
                      order by play_count desc
""")

# At what hour of the day are songs played the most?
popular_hour_query = ("""
                      select t.hour, count(*) as total_plays
                      from fact_songplays f
                      inner join dim_time t
                      on f.start_time = t.start_time
                      group by t.hour
                      order by total_plays desc
""")

# How many song plays come from free vs paid users?
subscription_usage_query = ("""
                            select level, count(*) as song_plays
                            from fact_songplays  
                            group by level
                            order by song_plays desc
""")

# Which locations have the most song plays?
top_locations_query = ("""
                      select location, count(*) as play_count
                      from fact_songplays
                      group by location
                      order by play_count desc
""")

# Which artist is most popular in each location?
top_artist_by_location_query = ("""
                                select * from (
                                select a.location, a.name as artist_name, count(*) as play_count,
                                row_number() over(partition by a.location order by count(*) desc) as rk
                                from fact_songplays f
                                inner join dim_artists a
                                on f.artist_id = a.artist_id
                                group by a.location, a.name
                              ) t
                              where rk = 1
""")

# Which day of the week do users listen to the most music
popular_weekday_query = ("""
                        select t.weekday, count(*) as total_plays
                        from fact_songplays f
                        join dim_time t
                        on f.start_time = t.start_time
                        group by t.weekday
                        order by total_plays desc;
""")



analytics_queries = [
    ("How many song plays exist in the system?", total_songplays_query),

    ("What are the top 10 most played songs?", top_songs_query),

    ("Which users listen to the most songs?", top_users_query),

    ("Which artists are played the most?", top_artists_query),

    ("At what hour of the day are songs played the most?", popular_hour_query),

    ("How many song plays come from free vs paid users?", subscription_usage_query),

    ("Which locations have the most song plays?", top_locations_query),

    ("Which artist is most popular in each location?", top_artist_by_location_query),

    ("Which day of the week do users listen to the most music?", popular_weekday_query)
]