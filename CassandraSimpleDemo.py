from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    print("hello")
except Exception as e:
    print(e)

try:
    session.execute("""CREATE KEYSPACE IF NOT EXISTS testkeyspace
            WITH REPLICATION = 
            {'class' : 'SimpleStrategy', 'replication_factor': 1}
    """)
except Exception as e:
    print(e)

try:
    session.set_keyspace('testkeyspace')
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS music_library"
query += "(year int, artist_name text, album_name text, PRIMARY KEY(year, artist_name))"

try:
    session.execute(query)
    print("Table 1 Executed")
except Exception as e:
    print(e)

query = "CREATE TABLE IF NOT EXISTS album_library"
query += "(year int, artist_name text, album_name text, PRIMARY KEY(artist_name, year))"

try:
    session.execute(query)
    print("Table 2 Executed")
except Exception as e:
    print(e)

query = "INSERT INTO music_library(year, artist_name, album_name)"
query += "VALUES (%s, %s, %s)"

try:
    session.execute(query,(1970, "The Beatles", "Let It Be"))
    print("Row Inserted")
except Exception as e:
    print(e)


query = "Select * from music_library WHERE year = 1970"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)

for row in rows:
    print(type(row))
    print(row.year, row.artist_name, row.album_name)

query = "drop table music_library"
try:
    rows = session.execute(query)
    print("Dropped Music Library")
except Exception as e:
    print(e)

query = "drop table album_library"
try:
    rows = session.execute(query)
    print("Dropped Album Library")
except Exception as e:
    print(e)

session.shutdown()
cluster.shutdown()