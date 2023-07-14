import os
import glob
import pandas as pd
from sql_queries import *
from create_tables import create_tables, drop_tables
from db_connector import connect_db
import datetime

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files

def process_song_data(conn, cur, filapath):
    song_files = get_files(filapath)
    for filepath in song_files:
        df = pd.read_json(filepath, lines=True)
        song_data = df[['song_id', 'title','artist_id', 'year', 'duration']].values[0].tolist()
        cur.execute(song_table_insert, song_data)
        conn.commit()
        
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
        cur.execute(artist_table_insert, artist_data)
        conn.commit()

def process_log_data(conn, cur, filepath):
    log_files = get_files(filepath)
    for filepath in log_files:
        df = pd.read_json(filepath, lines=True)
        df = df[df["page"]=='NextSong'].reset_index() # фильтр записией по NextSong
        t = pd.to_datetime(df.ts, unit='ms') # Конвертация ts из формата timestamp в datetime
        #Извлечение timestamp, hour, day, week of year, month, year и weekday из ts 
        # и запись в виде кортежа в переменную time_data
        df['week'] = t.apply(lambda x: datetime.date(x.year, x.month, x.day).isocalendar()[1])
        df['week_day'] = t.apply(lambda x: datetime.date(x.year, x.month, x.day).strftime("%A"))
        time_data = (t, t.dt.hour, t.dt.day, df.week, t.dt.month, t.dt.year, df.week_day) 
        column_labels = ['start_time','hour','day','week','month', 'year','weekday']
        # dataframe time_df , который включает в себя обработанную дату и колонки с остальной
        # информацией
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
        df['start_time'] = t
        # Загрузка данных в таблицу Time
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
            conn.commit()
        user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
            conn.commit()
        
        for index, row in df.iterrows():
            # получите songid и artistid из таблиц song и artist
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
            # извлеките start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            # запишите в songplay
            songplay_data = (str(row.start_time),int(row.userId),str(row.level),str(songid),str(artistid), int(row.sessionId),str(row.location),str(row.userAgent))
            cur.execute(songplay_table_insert, songplay_data)
            conn.commit()

def main():
    conn = connect_db()
    # открытие курсора
    cur = conn.cursor()
    # print(conn, cur)
    drop_tables(conn, cur)
    create_tables(conn, cur)
    
    song_path = r"./data/song_data/"
    log_path = r"./data/log_data/"
    
    process_song_data(conn, cur, song_path)
    process_log_data(conn, cur, log_path)

if __name__=="__main__":
    main()
    
    
    


    