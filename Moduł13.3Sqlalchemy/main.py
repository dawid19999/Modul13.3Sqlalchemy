

import csv
from sqlalchemy import create_engine, MetaData, Table, select, update, delete

engine = create_engine("sqlite:///database.db", echo=True)
conn = engine.connect()
metadata = MetaData()


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


stations = Table("stations", metadata, autoload_with=engine)
measurements = Table("measurements", metadata, autoload_with=engine)


with open("clean_stations.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    conn.execute(stations.insert(), list(reader))

with open("clean_measure.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    conn.execute(measurements.insert(), list(reader))


print("\nPierwsze 5 rekordów z tabeli stations:")
rows = conn.execute(select(stations).limit(5)).fetchall()
for row in rows:
    print(row)


print("\nStacje z USA:")
rows = conn.execute(
    select(stations).where(stations.c.country == "US").limit(5)
).fetchall()
for row in rows:
    print(row)


print("\nPomiary gdzie opady > 1.0:")
rows = conn.execute(
    select(measurements).where(measurements.c.precip > 1.0).limit(5)
).fetchall()
for row in rows:
    print(row)


print("\nAktualizacja nazwy stacji USC00519397...")
stmt = (
    update(stations)
    .where(stations.c.station == "USC00519397")
    .values(name="TEST STATION")
)
conn.execute(stmt)

rows = conn.execute(
    select(stations).where(stations.c.station == "USC00519397")
).fetchall()
print(rows)


print("\nUsuwam stację USC00513117...")
stmt = delete(stations).where(stations.c.station == "USC00513117")
conn.execute(stmt)

rows = conn.execute(
    select(stations).where(stations.c.station == "USC00513117")
).fetchall()
print(rows)


print("\nTabele w bazie danych:")
print(metadata.tables.keys())

conn.close()
