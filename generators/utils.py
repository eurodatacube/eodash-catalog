
import json
import re
from functools import reduce
from duration import Duration
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Iterator
from six import string_types
from owslib.wms import WebMapService
from dateutil import parser

ISO8601_PERIOD_REGEX = re.compile(
    r"^(?P<sign>[+-])?"
    r"P(?!\b)"
    r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
    r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
    r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
    r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
    r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$")
# regular expression to parse ISO duartion strings.

def create_geojson_point(lon, lat):
    point = {
        "type": "Point",
        "coordinates": [lon, lat]
    }

    feature = {
        "type": "Feature",
        "geometry": point,
        "properties": {}
    }

    feature_collection = {
        "type": "FeatureCollection",
        "features": [feature]
    }

    return feature_collection

def retrieveExtentFromWMS(capabilties_url, layer):
    times = []
    wms = None
    try:
        wms = WebMapService(capabilties_url, version='1.1.1')
        if layer in list(wms.contents) and wms[layer].timepositions != None:
            for tp in wms[layer].timepositions:
                tp_def = tp.split("/")
                if len(tp_def)>1:
                    dates = interval(
                        parser.parse(tp_def[0]),
                        parser.parse(tp_def[1]),
                        parse_duration(tp_def[2])
                    )
                    times += [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in dates]
                else:
                    times.append(tp)
            times = [time.replace('\n','').strip() for time in times]
            # get unique times
            times = reduce(lambda re, x: re+[x] if x not in re else re, times, [])
    except Exception as e:
        print("Issue extracting information from WMS capabilities")
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        print (message)

    bbox = [-180,-90,180,90]
    if wms and wms[layer].boundingBoxWGS84:
        bbox = [float(x) for x in wms[layer].boundingBoxWGS84]

    return {
        "spatial": bbox,
        "temporal": times,
    }

def interval(start: datetime, stop: datetime, delta: timedelta) -> Iterator[datetime]:
    while start <= stop:
        yield start
        start += delta
    yield stop

def parse_duration(datestring):
    """
    Parses an ISO 8601 durations into datetime.timedelta
    """
    if not isinstance(datestring, string_types):
        raise TypeError("Expecting a string %r" % datestring)
    match = ISO8601_PERIOD_REGEX.match(datestring)
    if not match:
        # try alternative format:
        if datestring.startswith("P"):
            durdt = parse_datetime(datestring[1:])
            if durdt.year != 0 or durdt.month != 0:
                # create Duration
                ret = Duration(days=durdt.day, seconds=durdt.second,
                               microseconds=durdt.microsecond,
                               minutes=durdt.minute, hours=durdt.hour,
                               months=durdt.month, years=durdt.year)
            else:  # FIXME: currently not possible in alternative format
                # create timedelta
                ret = timedelta(days=durdt.day, seconds=durdt.second,
                                microseconds=durdt.microsecond,
                                minutes=durdt.minute, hours=durdt.hour)
            return ret
        raise ISO8601Error("Unable to parse duration string %r" % datestring)
    groups = match.groupdict()
    for key, val in groups.items():
        if key not in ('separator', 'sign'):
            if val is None:
                groups[key] = "0n"
            # print groups[key]
            if key in ('years', 'months'):
                groups[key] = Decimal(groups[key][:-1].replace(',', '.'))
            else:
                # these values are passed into a timedelta object,
                # which works with floats.
                groups[key] = float(groups[key][:-1].replace(',', '.'))
    if groups["years"] == 0 and groups["months"] == 0:
        ret = timedelta(days=groups["days"], hours=groups["hours"],
                        minutes=groups["minutes"], seconds=groups["seconds"],
                        weeks=groups["weeks"])
        if groups["sign"] == '-':
            ret = timedelta(0) - ret
    else:
        ret = Duration(years=groups["years"], months=groups["months"],
                       days=groups["days"], hours=groups["hours"],
                       minutes=groups["minutes"], seconds=groups["seconds"],
                       weeks=groups["weeks"])
        if groups["sign"] == '-':
            ret = Duration(0) - ret
    return ret