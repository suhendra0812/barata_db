import enum
import os
import glob
import subprocess
from dateutil.parser import parse as dateparse
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Frame, Oil, Ship, Scene


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

uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(uri)

Frame.__table__.create(engine, checkfirst=True)
Oil.__table__.create(engine, checkfirst=True)
Ship.__table__.create(engine, checkfirst=True)
Scene.__table__.create(engine, checkfirst=True)

csk_dirs = glob.glob("E:/BARATA/seonse_outputs/cosmo_skymed/2021*/*")
rs_dirs = glob.glob("E:/BARATA/seonse_outputs/radarsat/2021*/*")
data_dirs = csk_dirs + rs_dirs
data_dirs.sort(key=sort_by_date)

for i, data_dir in enumerate(data_dirs):
    print(f"{i+1}/{len(data_dirs)}: {os.path.basename(data_dir)}")

    scene_list = glob.glob(os.path.join(data_dir, "*geo5.tif"))
    if len(scene_list) > 0:
        print("Insert scene...")
        for i, scene_path in enumerate(scene_list):
            scene_path = os.path.abspath(scene_path)
            scene_name = os.path.basename(scene_path)
            print(f"{i+1}/{len(scene_list)}: {scene_name}")

            cmd = f'raster2pgsql -s 4326 -I -C -x -e -M -t auto -a -l 4 "{scene_path}" -F public.scenes | psql "{uri}"'
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        Session = sessionmaker(engine)
        session = Session()

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

                print(f"{i+1}/{len(frame_gdf)}: {satellite} | {sensor} | {polarisation} | {datetime}")

                frame = Frame(
                    satellite=satellite,
                    sensor=sensor,
                    polarisation=polarisation,
                    datetime=datetime,
                    geom=geom,
                )

                session.add(frame)

                scenes = session.query(Scene).filter_by(
                    filename=os.path.basename(scene_path)
                )
                for scene in scenes:
                    scene.frame = frame
                    frame.scenes.append(scene)

                session.commit()

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

                        print(f"{i+1}/{len(oil_gdf)}: {area} | {confidence} | {datetime}")

                        oil = Oil(
                            length=length,
                            width=width,
                            area=area,
                            confidence=confidence,
                            datetime=datetime,
                            geom=geom,
                        )

                        oil.frame = frame
                        frame.oils.append(oil)

                        session.add(oil)
                        session.commit()

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

                        print(f"{i+1}/{len(ship_gdf)}: {length} | {mmsi} | {datetime}")

                        ship = Ship(
                            longitude=longitude,
                            latitude=latitude,
                            length=length,
                            width=width,
                            velocity=velocity,
                            heading=heading,
                            mmsi=mmsi,
                            datetime=datetime,
                            geom=geom,
                        )

                        ship.frame = frame
                        frame.ships.append(ship)

                        session.add(ship)
                        session.commit()
        session.close()
