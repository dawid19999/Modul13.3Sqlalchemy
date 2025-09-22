

from sqlalchemy import create_engine, Column, Integer, String, Float, and_
from sqlalchemy.orm import declarative_base  
from sqlalchemy.orm import sessionmaker


Base = declarative_base()
engine = create_engine('sqlite:///weather.db', echo=False)  
Session = sessionmaker(bind=engine)


class Station(Base):
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)
    state = Column(String)
    country = Column(String)
    timezone = Column(String, nullable=True)

class Measure(Base):
    __tablename__ = 'measure'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    date = Column(String)
    prcp = Column(Float, nullable=True)
    tobs = Column(Float, nullable=True)


Base.metadata.create_all(engine)


def insert_csv(cls, csv_file):
    session = Session()
    if session.query(cls).first() is None:
        with open(csv_file, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in row:
                    try:
                        row[key] = float(row[key])
                    except (ValueError, TypeError):
                        pass
                obj = cls(**row)
                session.add(obj)
        session.commit()
    session.close()


insert_csv(Station, "clean_stations.csv")
insert_csv(Measure, "clean_measure.csv")


def select_all(cls, limit=None):
    session = Session()
    q = session.query(cls)
    if limit:
        q = q.limit(limit)
    result = q.all()
    session.close()
    return result

def select_where(cls, **kwargs):
    session = Session()
    conds = [getattr(cls, k) == v for k, v in kwargs.items()]
    result = session.query(cls).filter(and_(*conds)).all()
    session.close()
    return result

def update(cls, row_id, **kwargs):
    session = Session()
    obj = session.query(cls).get(row_id)
    if obj:
        for k, v in kwargs.items():
            setattr(obj, k, v)
        session.commit()
    session.close()

def delete(cls, row_id):
    session = Session()
    obj = session.query(cls).get(row_id)
    if obj:
        session.delete(obj)
        session.commit()
    session.close()


if __name__ == "__main__":
    print("=== Pierwsze 5 stacji (ORM) ===")
    for s in select_all(Station, limit=5):
        print(s.__dict__)

    print("\n=== Pierwsze 5 pomiarów (ORM) ===")
    for m in select_all(Measure, limit=5):
        print(m.__dict__)

    print("\n=== Filtrowanie stacji w stanie 'HI' (ORM) ===")
    for s in select_where(Station, state='HI')[:5]:
        print(s.__dict__)
