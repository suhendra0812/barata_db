from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.types import Integer, Float, String, DateTime
from geoalchemy2.types import Raster, Geometry

Base = declarative_base()

class Oil(Base):
    __tablename__ = "oils"
    id = Column(Integer, primary_key=True)
    frame_id = Column(Integer, ForeignKey("frames.id"))
    frame = relationship("Frame", back_populates="oils")
    length = Column(Float)
    width = Column(Float)
    area = Column(Float)
    confidence = Column(String)
    datetime = Column(DateTime(timezone=True))
    geom = Column(Geometry())

    def __repr__(self):
        return f"<Oil(frame_id={self.frame_id}, area={self.area}, confidence={self.confidence}, datetime={self.datetime})>"

class Ship(Base):
    __tablename__ = "ships"
    id = Column(Integer, primary_key=True)
    frame_id = Column(Integer, ForeignKey("frames.id"))
    frame = relationship("Frame", back_populates="ships")
    longitude = Column(Float)
    latitude = Column(Float)
    length = Column(Float)
    width = Column(Float)
    velocity = Column(Float)
    heading = Column(Float)
    mmsi = Column(String)
    datetime = Column(DateTime(timezone=True))
    geom = Column(Geometry("POINT"))

    def __repr__(self):
        return f"<Ship(frame_id={self.frame_id}, longitude={self.longitude}, latitude={self.latitude}, length={self.length}, datetime={self.datetime})>"

class Scene(Base):
    __tablename__ = "scenes"
    rid = Column(Integer, primary_key=True)
    frame_id = Column(Integer, ForeignKey("frames.id"))
    frame = relationship("Frame", back_populates="scenes")
    filename = Column(String)
    rast = Column(Raster)

class Frame(Base):
    __tablename__ = "frames"
    id = Column(Integer, primary_key=True)
    satellite = Column(String)
    sensor = Column(String)
    polarisation = Column(String)
    datetime = Column(DateTime(timezone=True))
    scenes = relationship("Scene", back_populates="frame", order_by=Scene.rid)
    oils = relationship("Oil", back_populates="frame", order_by=Oil.id)
    ships = relationship("Ship", back_populates="frame", order_by=Ship.id)
    geom = Column(Geometry("POLYGON"))

    def __repr__(self):
        return f"<Frame(satellite={self.satellite}, sensor={self.sensor}, polarisation={self.polarisation}, datetime={self.datetime})>"