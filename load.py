import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import joinedload, lazyload, sessionmaker
from sqlalchemy.sql.functions import count

from postgis_models import Frame, Oil, Ship

load_dotenv()
user = os.getenv("PG_USER")
password = os.getenv("PG_PASS")
database = os.getenv("PG_DBNAME")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")

uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(uri)

Session = sessionmaker(engine)
session = Session()

q = session.query(Frame.id).\
            join(Frame.ships).\
            group_by(Frame.id).\
            having(count(Ship.id) > 0)

statement = q.statement