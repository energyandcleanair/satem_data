from sqlalchemy import create_engine, Column, Integer, DateTime, Float, engine, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from decouple import config


engine = None
Base = declarative_base(engine)


def get_engine():
    global engine
    if engine is None:
        engine = create_engine(config('CREA_POSTGRES_URL'))
    return engine


class Facilities(Base):
    __tablename__ = "facilities"

    source             = Column('source'             ,Text)
    orig_facility_id   = Column('orig_facility_id'   ,Text)
    name               = Column('name'               ,Text)
    lat            = Column('lat'            ,Float (precision=4))
    lon            = Column('lon'            ,Float (precision=4))
    geom               = Column('geom'               ,Geometry('point', srid=4326))
    id                 = Column('id'                 ,Text, primary_key=True)
    country            = Column('country'            ,Text)


class Units(Base):
    __tablename__ = "units"

    facility_id   = Column('facility_id'            ,Text,
                           ForeignKey('facilities.id', ondelete = 'CASCADE'))
    orig_unit_id       = Column('orig_unit_id'      ,Text)
    lat           = Column('lat'                    ,Float (32))
    lon           = Column('lon'                    ,Float (32))
    geom          = Column('geom'                   ,Geometry('point', srid=4326))
    id            = Column('id'                     ,Text, primary_key=True)

    facilities = relationship('Facilities', backref='units')


class Emissions(Base):
    __tablename__ = "emissions"

    id            = Column('id'           ,Integer, primary_key=True)
    unit_id       = Column('unit_id'      ,Text, 
                           ForeignKey('units.id', ondelete = 'CASCADE'))
    date          = Column('date'         ,DateTime(timezone = True))
    pollutant     = Column('pollutant'    ,Text)
    emission      = Column('emission'     ,Float(32))
    unit          = Column('unit'         ,Text)

    __table_args__ = (
    # this can be db.PrimaryKeyConstraint if you want it to be a primary key
    UniqueConstraint('unit_id', 'date', 'pollutant'),
    )

    units      = relationship('Units', backref='emissions')