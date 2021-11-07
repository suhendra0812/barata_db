import os
import glob
from dateutil.parser import parse as dateparse
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Frame, Oil, Ship

def sort_by_date(x):
    fname = os.path.basename(x)
    dt_str = "".join(fname.split("_")[1:])
    dt = dateparse(dt_str)
    return dt
    
load_dotenv()
user = os.getenv("PG_USER")
password = os.getenv("PG_PASS")
database = os.getenv("PG_DBNAME")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

Frame.__table__.create(engine, checkfirst=True)
Oil.__table__.create(engine, checkfirst=True)
Ship.__table__.create(engine, checkfirst=True)

Session = sessionmaker(engine)
session = Session()

csk_dirs = glob.glob("F:/BARATA/seonse_outputs/cosmo_skymed/2021*/*")
rs_dirs = glob.glob("F:/BARATA/seonse_outputs/radarsat/2021*/*")
data_dirs = csk_dirs + rs_dirs
data_dirs.sort(key=sort_by_date)

for i, data_dir in enumerate(data_dirs):
    print(f"{i+1}/{len(data_dirs)}: {os.path.basename(data_dir)}")
    frame_dir = data_dir.replace("seonse_outputs", "frames")

    frame_list = glob.glob(os.path.join(frame_dir, "*frame.shp"))

    if len(frame_list) > 0:
        print("Insert frame...")
        frame_gdf = gpd.read_file(frame_list[0])
        for i, row in frame_gdf.iterrows():
            satellite = row["SATELLITE"]
            sensor = row["SENSOR"]
            polarisation = row["POLARISASI"]
            datetime = pd.to_datetime(row["DATETIME"], utc=True).to_pydatetime()
            geom = row.geometry.wkt

            frame = Frame(
                satellite=satellite,
                sensor=sensor,
                polarisation=polarisation,
                datetime=datetime,
                geom=geom
            )

            session.add(frame)

            oil_list = glob.glob(os.path.join(data_dir, "*OIL.shp"))
            if len(oil_list) > 0:
                print("Insert oil...")
                oil_gdf = gpd.read_file(oil_list[0])
                for i, row in oil_gdf.iterrows():
                    length = row["LENGTH_KM"]
                    width = row["WIDTH_KM"]
                    area = row["AREA_KM"]
                    confidence = row["ALARM_LEV"]
                    datetime = row["DATE-TIME"]
                    geom = row.geometry.wkt

                    oil = Oil(
                        length=length,
                        width=width,
                        area=area,
                        confidence=confidence,
                        datetime=datetime,
                        geom=geom
                    )

                    oil.frame = frame
                    frame.oils.append(oil)

                    session.add(oil)

            ship_list = glob.glob(os.path.join(data_dir, "*SHIP.shp"))
            if len(ship_list) > 0:
                print("Insert ship...")
                ship_gdf = gpd.read_file(ship_list[0])
                for i, row in ship_gdf.iterrows():
                    longitude = row["LON_CENTRE"]
                    latitude = row["LAT_CENTRE"]
                    length = row["LENGTH"]
                    width = row["WIDTH"]
                    velocity = row["TARGET_VEL"]
                    heading = row["TARGET_DIR"]
                    mmsi = row["AIS_MMSI"]
                    datetime = row["TARGET_UTC"]
                    geom = row.geometry.wkt

                    ship = Ship(
                        longitude=longitude,
                        latitude=latitude,
                        length=length,
                        width=width,
                        velocity=velocity,
                        heading=heading,
                        mmsi=mmsi,
                        datetime=datetime,
                        geom=geom
                    )

                    ship.frame = frame
                    frame.ships.append(ship)

                    session.add(ship)

session.commit()