import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float

# Utwórz silnik SQLite i metadata
engine = create_engine("sqlite:///stations.db", echo=True)
metadata = MetaData()

# Definicja tabeli
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

# Tworzenie tabeli w bazie (jeśli nie istnieje)
metadata.create_all(engine)

# Funkcja do wczytania danych z CSV
def load_csv_to_table(csv_file, table, engine):
    rows = []
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "station": row["station"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "elevation": float(row["elevation"]),
                "name": row["name"],
                "country": row["country"],
                "state": row["state"]
            })
    with engine.begin() as conn:
        conn.execute(table.delete())  # usuwa wszystkie rekordy z tabeli
        conn.execute(table.insert(), rows)  # wstawia nowe dane

# Wczytaj dane z CSV
load_csv_to_table("clean_stations.csv", stations_table, engine)

# Przykładowe zapytanie SELECT
from sqlalchemy import select

with engine.connect() as conn:
    stmt = select([stations_table])  # kolumny w liście
    results = conn.execute(stmt).fetchall()
    for row in results:
        print(row)