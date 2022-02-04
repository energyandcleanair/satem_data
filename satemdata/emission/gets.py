import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import warnings

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import func

from .db import engine, Facilities, Units, Emissions



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


def get_emissions(date_from, date_to, facility_id=None, unit_id=None, tz = 'UTC', pollutant = None, sum=False):
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
    sum: boolean.
        Whether or not to sum emissions for each unit
    """
    session = scoped_session(sessionmaker(autocommit=False,autoflush=False,bind=engine))

    assert (facility_id is None) or isinstance(facility_id, str) or isinstance(facility_id, list)
    assert (unit_id is None) or isinstance(unit_id, str) or isinstance(unit_id, list)
    assert isinstance(date_from, str)
    assert isinstance(date_to  , str)
    assert isinstance(tz, str)
    assert (pollutant is None) or isinstance(pollutant, list) or isinstance(pollutant, str)

    if facility_id and isinstance(facility_id, str):
        facility_id = [facility_id]
    if unit_id and isinstance(unit_id, str):
        unit_id = [unit_id]
    if pollutant is None:
        pollutant = ['nox', 'so2']
    elif isinstance(pollutant, str):
        pollutant = [pollutant]

    try:
        tz = pytz.timezone(tz)
    except:
        raise ValueError('The input tz string is not recognizable by pytz.timezone')

    date_from = tz.localize(datetime.strptime(date_from, '%Y-%m-%d')).strftime('%Y-%m-%d 00:00:00 %z')
    date_to   = tz.localize(datetime.strptime(date_to  , '%Y-%m-%d')).strftime('%Y-%m-%d 23:59:59 %z')

    if sum:
        emissions = session.query(Units.facility_id,
                                  Facilities.lon_avg,
                                  Facilities.lat_avg,
                                  Emissions.unit_id,
                                  Emissions.pollutant,
                                  Emissions.unit,
                                  func.sum(Emissions.emission).label('emission')) \
            .select_from(Emissions) \
            .join(Units).join(Facilities) \
            .group_by(Facilities.lon_avg,
                      Facilities.lat_avg,
                      Units.facility_id, Emissions.unit_id, Emissions.pollutant, Emissions.unit)
    else:
        emissions = session.query(Units.facility_id,
                                  Facilities.lon_avg,
                                  Facilities.lat_avg,
                                  Emissions.unit_id,
                                  Emissions.date,
                                  Emissions.pollutant,
                                  Emissions.unit,
                                  Emissions.emission) \
            .select_from(Emissions) \
            .join(Units).join(Facilities)

    emissions = emissions.filter((Emissions.date >= date_from) \
                                 & (Emissions.date <= date_to) \
                                 & (Emissions.pollutant.in_(pollutant)))

    if unit_id is not None:
            emissions = emissions.filter(Emissions.unit_id.in_(unit_id))

    if facility_id is not None:
            emissions = emissions.filter(Units.facility_id.in_(facility_id))

    emissions = emissions.all()

    if len(emissions) == 0:
        warnings.warn('There is no emissions data matching criteria unit_id = {}, date_from = {}, date_to = {}, and pollutant = {}. Anything wrong?'.format(str(unit_id), date_from, date_to, str(pollutant)))
        return pd.DataFrame()

    emissions = pd.DataFrame(emissions)
    session.close()
    return emissions
