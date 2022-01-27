from db import engine, Facilities, Units, Emissions, Base
from sqlalchemy.orm import sessionmaker, scoped_session
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
import pytz


def get_facilities(country = None):
    """
    params
    ------
    country: str
        The ISO2 code of the country to retrieve facilities list from.
    """
    assert isinstance(country, str)

    session = scoped_session(sessionmaker(autocommit=False,autoflush=False,bind=engine))
    if country is None:
        facs        = session.query(Facilities).all()
    else:
        facs        = session.query(Facilities).filter(Facilities.country == country).all()

    if len(facs) == 0:
        warnings.warn('There is no data matching criteria country = {}. Anything wrong?'.format(country))
        return pd.DataFrame()

    facs = pd.DataFrame([(ff.source, ff.orig_facility_id, ff.name, ff.lat_avg, ff.lon_avg,
                          ff.id, ff.country) for ff in facs],
                        columns = ['source','orig_facility_id','name','lat','lon','id','country'])
    session.close()
    return facs


def get_units(facility_id=None, country=None):
    """
    params
    ------
    country: str
        The ISO2 code of the country to retrieve units list from.
    facility_id: str or list
        The ids of the facilities to retrieve units list from.
    """
    session = scoped_session(sessionmaker(autocommit=False,autoflush=False,bind=engine))

    assert (facility_id is None) or isinstance(facility_id, str) or isinstance(facility_id, list)
    assert (country is None) or isinstance(country, str)

    if country is None:
        if facility_id is None:
            units   = session.query(Units).all()

        elif isinstance(facility_id, str):
            units   = session.query(Units).filter(Units.facility_id == facility_id).all()

        else:
            facs  = [str(x) for x in facility_id]
            units   = session.query(Units).filter(Units.facility_id.in_(facs)).all()

    else:
        if facility_id is None:
            facs        = session.query(Facilities).filter(Facilities.country == country).all()
            facs        = [ff.id for ff in facs]
            units       = session.query(Units).filter(Units.facility_id.in_(facs)).all()

        elif isinstance(facility_id, str):
            facs        = session.query(Facilities).filter(Facilities.country == country).all()
            facs        = [ff.id for ff in facs if ff.id == facility_id]
            units       = session.query(Units).filter(Units.facility_id.in_(facs)).all()

        else:
            facs        = session.query(Facilities).filter(Facilities.country == country).all()
            facs        = [ff.id for ff in facs if ff.id in [str(x) for x in facility_id]]
            units        = session.query(Units).filter(Units.facility_id.in_(facs)).all()

    if len(units) == 0:
        warnings.warn('There is no data matching criteria facility_id = {} and country = {}. Anything wrong?'.format(str(facility_id), country))
        return pd.DataFrame()

    units = pd.DataFrame([(uu.facility_id, uu.orig_unit_id, uu.lat, uu.lon, uu.id) for uu in units],
                         columns = ['facility_id','orig_unit_id','lat','lon','id'])
    session.close()
    return units


def get_emissions(unit_id, date_from, date_to, tz = 'UTC', pollutant = None):
    """
    params
    ------
    unit_id: str or list
        The ids of the units to retrieve emission data from.
    date_from: str, yyyy-mm-dd
        The start date of the emission data.
    date_to: str, yyyy-mm-dd
        The end date of the emission data.
    tz: pytz.timezone recognizable string
        The time zone of the specified date_from and date_to.
    pollutant: str or list.
        'nox','so2',or ['nox','so2']
    """
    session = scoped_session(sessionmaker(autocommit=False,autoflush=False,bind=engine))

    assert isinstance(unit_id, str) or isinstance(unit_id, list)
    assert isinstance(date_from, str)
    assert isinstance(date_to  , str)
    assert isinstance(tz, str)
    assert (pollutant is None) or isinstance(pollutant, list) or isinstance(pollutant, str)

    if isinstance(unit_id, str):
        unit_id = [unit_id]
    if pollutant is None:
        pollutant = ['nox','so2']
    elif isinstance(pollutant, str):
        pollutant = [pollutant]

    try:
        tz = pytz.timezone(tz)
    except:
        raise 'The input tz string is not recognizable by pytz.timezone'

    date_from = tz.localize(datetime.strptime(date_from, '%Y-%m-%d')).strftime('%Y-%m-%d 00:00:00 %z')
    date_to   = tz.localize(datetime.strptime(date_to  , '%Y-%m-%d')).strftime('%Y-%m-%d 23:59:59 %z')

    emissions = session.query(Emissions).filter((Emissions.unit_id.in_(unit_id)) & (Emissions.time >= date_from) & (Emissions.time <= date_to) & (Emissions.pollutant.in_(pollutant))).all()

    if len(emissions) == 0:
        warnings.warn('There is no emissions data matching criteria unit_id = {}, date_from = {}, date_to = {}, and pollutant = {}. Anything wrong?'.format(str(unit_id), date_from, date_to, str(pollutant)))
        return pd.DataFrame()

    emissions = pd.DataFrame([(ee.unit_id, ee.time, ee.emission, ee.pollutant, ee.unit) \
                               for ee in emissions],
                            columns = ['unit_id','time','emission','pollutant','unit'])

    session.close()
    return emissions