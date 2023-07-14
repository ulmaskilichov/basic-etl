from db_connector import connect_db

conn = connect_db()
# открытие курсора
cur = conn.cursor()

songplays = "SELECT * FROM songplays LIMIT 5"

users = "SELECT * FROM users LIMIT 5"

songs = "SELECT * FROM songs LIMIT 5"

time = "SELECT * FROM time_table LIMIT 5"

tests = [songplays, users, songs, time]
for test in tests:
    cur.execute(test)
    ans = cur.fetchall()
    print("\n\n\t\t<--========================-->\n\n")
    print(f"executing {test}")
    print(*ans, sep='\n')
