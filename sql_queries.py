songplays_table = """
    CREATE TABLE songplays
    (
        songplay_id serial PRIMARY KEY
        ,start_time DATE
        ,user_id INT
        ,level TEXT
        ,song_id TEXT
        ,artist_id TEXT
        ,session_id INT
        ,location TEXT
        ,user_agent TEXT
    )
"""

users_table = """
    CREATE TABLE users(
        user_id INT PRIMARY KEY
        ,first_name TEXT
        ,last_name TEXT
        ,gender TEXT
        ,level TEXT
    )
"""

songs_table = """
    CREATE TABLE songs(
        song_id TEXT PRIMARY KEY
        ,title TEXT
        ,artist_id TEXT
        ,year INT
        ,duration FLOAT
    )
"""

artists_table = """
    CREATE TABLE artists(
        artist_id TEXT PRIMARY KEY
        ,name TEXT
        ,location TEXT
        ,lattitude FLOAT
        ,longitude FLOAT
        ,time DATE
    )
"""

time_table = """
    CREATE TABLE time_table(
        start_time DATE
        ,hour INT
        ,day INT
        ,week INT
        ,month INT
        ,year INT
        ,weekday TEXT
    )
"""

tables = [
    songplays_table,
    songs_table,
    artists_table,
    time_table,
    users_table
]

song_table_insert = """INSERT INTO songs(song_id, title, artist_id,year,duration)\n 
                        VALUES(%s, %s, %s, %s, %s)\n ON CONFLICT (song_id) DO NOTHING"""

artist_table_insert = """INSERT INTO artists(artist_id, name,location,lattitude, longitude)\n 
                        VALUES(%s, %s, %s, %s, %s)\n ON CONFLICT (artist_id) DO NOTHING"""
                        
time_table_insert = """INSERT INTO time_table(start_time,hour,day,week,month, year,weekday)\n
                        VALUES(%s, %s, %s, %s, %s, %s, %s)"""
                        
user_table_insert = """INSERT INTO users(user_id,first_name, last_name, gender, level)\n
                        VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING"""

song_select = """SELECT songs.song_id, artists.artist_id FROM songs\n
                JOIN artists ON songs.artist_id=artists.artist_id WHERE\n
                songs.title=%s AND artists.name=%s AND songs.duration=%s"""

songplay_table_insert = """INSERT INTO songplays(start_time, user_id, level, artist_id, song_id,\n
                        session_id, location, user_agent)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s) on conflict(songplay_id) DO nothing"""
