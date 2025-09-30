

import csv
from sqlalchemy import create_engine, MetaData, Table, select, update, delete

engine = create_engine("sqlite:///database.db", echo=True)
metadata = MetaData()

stations = Table("stations", metadata)
measurements = Table("measurements", metadata)

def main():
    metadata.drop_all(engine)
    metadata.create_all(engine)

    metadata.reflect(bind=engine)
    stations_ref = metadata.tables["stations"]
    measurements_ref = metadata.tables["measurements"]

    with engine.begin() as conn:
        with open("clean_stations.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            conn.execute(stations_ref.insert(), list(reader))

        with open("clean_measure.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            conn.execute(measurements_ref.insert(), list(reader))

        print("\nPierwsze 5 rekordów z tabeli stations:")
        rows = conn.execute(select(stations_ref).limit(5)).fetchall()
        for row in rows:
            print(row)

        print("\nStacje z USA:")
        rows = conn.execute(
            select(stations_ref).where(stations_ref.c.country == "US").limit(5)
        ).fetchall()
        for row in rows:
            print(row)

        print("\nPomiary gdzie opady > 1.0:")
        rows = conn.execute(
            select(measurements_ref).where(measurements_ref.c.precip > 1.0).limit(5)
        ).fetchall()
        for row in rows:
            print(row)

        print("\nAktualizacja nazwy stacji USC00519397...")
        stmt = (
            update(stations_ref)
            .where(stations_ref.c.station == "USC00519397")
            .values(name="TEST STATION")
        )
        conn.execute(stmt)

        rows = conn.execute(
            select(stations_ref).where(stations_ref.c.station == "USC00519397")
        ).fetchall()
        print(rows)

        print("\nUsuwam stację USC00513117...")
        stmt = delete(stations_ref).where(stations_ref.c.station == "USC00513117")
        conn.execute(stmt)

        rows = conn.execute(
            select(stations_ref).where(stations_ref.c.station == "USC00513117")
        ).fetchall()
        print(rows)

        print("\nTabele w bazie danych:")
        print(metadata.tables.keys())

if __name__ == "__main__":
    main()

