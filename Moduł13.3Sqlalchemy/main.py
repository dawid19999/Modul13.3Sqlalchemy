
import csv
from sqlalchemy import create_engine


engine = create_engine("sqlite:///database.db", echo=True)
conn = engine.connect()


conn.execute("DROP TABLE IF EXISTS stations")
conn.execute("DROP TABLE IF EXISTS measurements")

conn.execute("""
CREATE TABLE stations (
    station TEXT,
    latitude REAL,
    longitude REAL,
    elevation REAL,
    name TEXT,
    country TEXT,
    state TEXT
)
""")

conn.execute("""
CREATE TABLE measurements (
    station TEXT,
    date TEXT,
    precip REAL,
    tobs REAL
)
""")


with open("clean_stations.csv", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader) 
    conn.execute(
        "INSERT INTO stations (station, latitude, longitude, elevation, name, country, state) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        list(reader)
    )

with open("clean_measure.csv", newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    conn.execute(
        "INSERT INTO measurements (station, date, precip, tobs) "
        "VALUES (?, ?, ?, ?)",
        list(reader)
    )


print("\nPierwsze 5 rekordów z tabeli stations:")
rows = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
for row in rows:
    print(row)

print("\nStacje z USA:")
rows = conn.execute("SELECT * FROM stations WHERE country = 'US' LIMIT 5").fetchall()
for row in rows:
    print(row)

print("\nPomiary gdzie opady > 1.0:")
rows = conn.execute("SELECT * FROM measurements WHERE precip > 1.0 LIMIT 5").fetchall()
for row in rows:
    print(row)


print("\nAktualizacja nazwy stacji USC00519397...")
conn.execute("UPDATE stations SET name = 'TEST STATION' WHERE station = 'USC00519397'")

rows = conn.execute("SELECT * FROM stations WHERE station = 'USC00519397'").fetchall()
print(rows)


print("\nUsuwam stację USC00513117...")
conn.execute("DELETE FROM stations WHERE station = 'USC00513117'")

rows = conn.execute("SELECT * FROM stations WHERE station = 'USC00513117'").fetchall()
print(rows)  


print("\nTabele w bazie danych:")
print(engine.table_names())

conn.close()
