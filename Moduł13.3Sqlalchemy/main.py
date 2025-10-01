
import csv
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey, select, update, delete
)

engine = create_engine("sqlite:///database.db", echo=True)

metadata = MetaData()

stations = Table(
    "stations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("station", String, unique=True, nullable=False),
    Column("name", String),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("state", String),
    Column("country", String),
)

measurements = Table(
    "measurements",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("station_id", Integer, ForeignKey("stations.id")),  
    Column("date", Date),
    Column("precip", Float),   
    Column("tobs", Float),     
)


def main():
    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.begin() as conn:
        with open("clean_stations.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            stations_data = []
            for row in reader:
                stations_data.append({
                    "station": row["station"],
                    "name": row.get("name"),
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"]),
                    "elevation": float(row["elevation"]),
                    "state": row["state"],
                    "country": row["country"]
                })
            conn.execute(stations.insert(), stations_data)

        with open("clean_measure.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            measurements_data = []
            for row in reader:
                measurements_data.append({
                    "station_id": conn.execute(
                        select(stations.c.id).where(stations.c.station == row["station"])
                    ).scalar(),
                    "date": row["date"],
                    "precip": float(row["precip"]) if row["precip"] else None,
                    "tobs": float(row["tobs"]) if row["tobs"] else None,
                })
            conn.execute(measurements.insert(), measurements_data)

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
        print(conn.execute(select(stations).where(stations.c.station == "USC00519397")).fetchall())

        print("\nUsuwam stację USC00513117...")
        stmt = delete(stations).where(stations.c.station == "USC00513117")
        conn.execute(stmt)
        print(conn.execute(select(stations).where(stations.c.station == "USC00513117")).fetchall())

        print("\nTabele w bazie danych:")
        print(metadata.tables.keys())


if __name__ == "__main__":
    main()


