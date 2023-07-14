from sql_queries import tables
DROP_TABLES = ['artists', 'songplays', 'users', 'time_table', 'songs']
def drop_tables(conn, cur):
    for table in DROP_TABLES:
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (table,))
        if cur.fetchone()[0]:
            cur.execute(f"DROP TABLE {table}")
            conn.commit()

def create_tables(conn, cur, tables=tables):
    for table in tables:
        cur.execute(table)
        conn.commit()