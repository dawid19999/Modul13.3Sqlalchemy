import os
import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float


if os.path.exists("stations.db"):
    os.remove("stations.db")


engine = create_engine("sqlite:///stations.db", echo=True)
metadata = MetaData()


stations_table = Table(
    "stations", metadata,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String)
)


measurements_table = Table(
    "measurements", metadata,
    Column("station", String),
    Column("date", String),
    Column("precip", Float),
    Column("tobs", Float)
)


metadata.create_all(engine)

def load_csv_to_table(csv_file, table, engine):
    rows = []
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_data = {}
            for k, v in row.items():
                # Zamiana na float jeśli możliwe
                try:
                    row_data[k] = float(v)
                except ValueError:
                    row_data[k] = v
            rows.append(row_data)
    with engine.begin() as conn:
        conn.execute(table.delete())  
        conn.execute(table.insert(), rows)  

if __name__ == "__main__":
    load_csv_to_table("clean_stations.csv", stations_table, engine)
    load_csv_to_table("clean_measure.csv", measurements_table, engine)

    with engine.connect() as conn:
        print("Pierwsze 5 rekordów z tabeli stations:")
        result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
        for row in result:
            print(row)

        print("\nPierwsze 5 rekordów z tabeli measurements:")
        result = conn.execute("SELECT * FROM measurements LIMIT 5").fetchall()
        for row in result:
            print(row)

        
        print("\nStacje w Polsce:")
        result = conn.execute("SELECT * FROM stations WHERE country='PL'").fetchall()
        for row in result:
            print(row)

        
        conn.execute(
            "UPDATE stations SET elevation=200.0 WHERE station=(SELECT station FROM stations LIMIT 1)"
        )
        updated_row = conn.execute(
            "SELECT * FROM stations WHERE station=(SELECT station FROM stations LIMIT 1)"
        ).fetchone()
        print("\nPo aktualizacji pierwszej stacji:")
        print(updated_row)

        
        second_station = conn.execute(
            "SELECT station FROM stations LIMIT 1 OFFSET 1"
        ).fetchone()[0]
        conn.execute(f"DELETE FROM stations WHERE station='{second_station}'")
        deleted_row = conn.execute(
            f"SELECT * FROM stations WHERE station='{second_station}'"
        ).fetchone()
        print(f"\nPo usunięciu stacji {second_station}:")
        print(deleted_row)
