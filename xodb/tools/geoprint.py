"""Geoprint is a geohash variant.

This algorithm is almost exactly identical to, and originally based
on, the Geohash module on Pypi by Leonard Norrgard, which in turn,
implements the geohash algorithm developed by Gustavo Niemeyer and
described in the wikipedia article:

http://en.wikipedia.org/wiki/Geohash

The main difference between this encoding and geohash is that it uses
a much smaller 'alphabet' of encoding characters to spell a hash,
instead of base32 encoding it uses simpler base4 encoding that uses
four possible states to encode each character instead of 32.  This
results in longer, uglier, less compressed geohashes but they are more
useful for doing proximity searching using hash prefixes.  Note that
like geohashes, there are edge cases near the equator and meridians
where points near each other have different prefixes.

All geoprints begin with either a 'w' or 'e' character to specify that
the hash belongs in the western or eastern hemisphere.  The western
hemisphere includes all longitudes less than zero down to -180.0, and
the eastern hemisphere includes all longitudes greater than or equal
to zero, up to 180.0.  Subsequent hash characters are one of g, a, t,
or c.
"""

from collections import namedtuple
from operator import neg, mod
from math import radians as rads
from math import (
    asin,
    atan2,
    cos,
    degrees,
    log10,
    pi,
    sin,
    sqrt,
    )

interval = namedtuple("interval", "min max")

alphabet = 'gatc'
decodemap = dict((k, i) for (i, k) in enumerate(alphabet))

EARTH_RADIUS = 6378100


def encode(latitude, longitude, precision=22, radians=False, box=False):
    """Encode the given latitude and longitude into a geoprint that
    contains 'precision' characters.

    If radians is True, input parameters are in radians, otherwise
    they are degrees.  Example::

    >>> c = (7.0625, -95.677068)
    >>> h = encode(*c)
    >>> h
    'watttatcttttgctacgaagt'

    >>> r = rads(c[0]), rads(c[1])
    >>> h2 = encode(*r, radians=True)
    >>> h == h2
    True
    """
    if radians:
        latitude = degrees(latitude)
        longitude = degrees(longitude)
    if longitude < 0:
        geoprint = ['w']
        loni = interval(-180.0, 0.0)
    else:
        geoprint = ['e']
        loni = interval(0.0, 180.0)

    lati = interval(-90.0, 90.0)

    while len(geoprint) < precision:
        ch = 0
        mid = (loni.min + loni.max) / 2
        if longitude > mid:
            ch |= 2
            loni = interval(mid, loni.max)
        else:
            loni = interval(loni.min, mid)

        mid = (lati.min + lati.max) / 2
        if latitude > mid:
            ch |= 1
            lati = interval(mid, lati.max)
        else:
            lati = interval(lati.min, mid)

        geoprint.append(alphabet[ch])
    result = ''.join(geoprint)
    if box:
        return (result, (lati[0], loni[0]), (lati[1], loni[1]))
    return result


def decode(geoprint, radians=False, box=False):
    """Decode a geoprint, returning the latitude and longitude.  These
    coordinates should approximate the input coordinates within a
    degree of error returned by 'error()'

    >>> c = (7.0625, -95.677068)
    >>> h = encode(*c)
    >>> c2 = decode(h)
    >>> e = error(h)
    >>> abs(c[0] - c2[0]) <= e
    True
    >>> abs(c[1] - c2[1]) <= e
    True

    If radians is True, results are in radians instead of degrees.

    >>> c2 = decode(h, radians=True)
    >>> e = error(h, radians=True)
    >>> abs(rads(c[0]) - c2[0]) <= e
    True
    >>> abs(rads(c[1]) - c2[1]) <= e
    True
    """
    lati = interval(-90.0, 90.0)
    first = geoprint[0]
    if first == 'w':
        loni = interval(-180.0, 0.0)
    elif first == 'e':
        loni = interval(0.0, 180.0)

    geoprint = geoprint[1:]

    for c in geoprint:
        cd = decodemap[c]
        if cd & 2:
            loni = interval((loni.min + loni.max) / 2, loni.max)
        else:
            loni = interval(loni.min, (loni.min + loni.max) / 2)
        if cd & 1:
            lati = interval((lati.min + lati.max) / 2, lati.max)
        else:
            lati = interval(lati.min, (lati.min + lati.max) / 2)
    lat = (lati.min + lati.max) / 2
    lon = (loni.min + loni.max) / 2
    if radians:
        lati = interval(rads(lati.min), rads(lati.max))
        loni = interval(rads(loni.min), rads(loni.max))
        lat, lon = rads(lat), rads(lon)
    if box:
        return (geoprint, (lati[0], loni[0]), (lati[1], loni[1]))
    return lat, lon


def error(geoprint, radians=False):
    """Returns the error of a given geoprint.

    If radians is true, return the error in radians, otherwise
    degrees.
    """
    e = 90.0 * pow(2, neg(len(geoprint) - 1))
    if radians:
        return rads(e)
    return e


def size(geoprint):
    """Return the *approximate* size in meters of one side of a
    geoprint box.
    """
    return 20000000.0 * pow(2, neg(len(geoprint) - 1))


def distance(start, end, radians=False):
    """
    Calculate the *approximate* distance between two geoprints.  Based
    on Haversine formula
    (http://en.wikipedia.org/wiki/Haversine_formula).

    If radians is True, return distance is radians on a approximate
    great circle, otherwise return meters.
    """
    start_lat, start_lon = decode(start, radians=True)
    end_lat, end_lon = decode(end, radians=True)
    d_lat = end_lat - start_lat
    d_long = end_lon - start_lon
    a = (sin(d_lat / 2) ** 2 + cos(start_lat) *
         cos(end_lat) * sin(d_long / 2) ** 2)
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    if radians:
        return c
    return EARTH_RADIUS * c


def bearing(start, end, radians=False):
    """
    Calculate *approximate* bearing between two geoprints.

    If radians is True, returns bearing in radians, otherwise degrees.
    """
    start_lat, start_lon = decode(start, radians=True)
    end_lat, end_lon = decode(end, radians=True)
    d_lon = end_lon - start_lon
    c = atan2(sin(d_lon) * cos(end_lat),
              cos(start_lat) * sin(end_lat) -
              sin(start_lat) * cos(end_lat) * cos(d_lon))
    if radians:
        return c
    return degrees(c)


def format(geoprint):
    """Return two formatted strings of latitude and longitude,
    truncated to the number of decimal places known based on the
    computed error in the geoprint.
    """
    lat, lon = decode(geoprint)
    err = max(1, int(round(-log10(error(geoprint))))) - 1
    lats = "%.*f" % (err, lat)
    lons = "%.*f" % (err, lon)
    if '.' in lats:
        lats = lats.rstrip('0')
    if '.' in lons:
        lons = lons.rstrip('0')
    return lats, lons


_radials = {'N' : 0,              # N
            'NE': pi / 4,         # NE
            'E' : pi / 2,         # E
            'SE': (3 * pi / 4),   # SE
            'S' : pi,             # S
            'SW': (5 * pi / 4),   # SW
            'W' : (3 * pi / 2),   # W
            'NW': (7 * pi / 4),   # NW
            }


def neighbors(geoprint, bearing=True, box=False):
    results = set()
    precision = len(geoprint)
    spacing = 180 / (2.0 ** (precision - 1))
    ctr = decode(geoprint)
    dirs = ['N', 'S', 'E', 'NE', 'SE', 'W', 'NW', 'SW']
    i = 0
    for direction_lat in (0, 1, -1):
        for direction_long in (0, 1, -1):
            if direction_lat == 0 and direction_long == 0:
                continue
            lat = ctr[0] + (direction_lat * spacing)
            lon = ctr[1] + (direction_long * spacing)
            h = encode(lat, lon, precision=precision, box=box)
            if bearing:
                h = (dirs[i], h)
            results.add(h)
            i += 1
    return results


def adjacent(first, second):
    """
    Return a flag indicating if two geoprints are adjacent to each
    other.
    """
    if first == second:
        return False
    if min(len(first), len(second)) < 3:
        raise TypeError(
            "Adjacency requires at least 3 characters of precision.")
    if len(first) != len(second):
        raise TypeError('Adjacency can only be checked for the same precision')
    lat1, lng1 = decode(first)
    lat2, lng2 = decode(second)
    precision = len(first)
    spacing = 180 / (2.0 ** (precision - 1))

    return ((abs(lat1 - lat2) == spacing and (lng1 == lng2)) or
            (abs(lng1 - lng2) == spacing and (lat1 == lat2)) or
            (abs(lat1 - lat2) == spacing and
             abs(lng1 - lng2) == spacing))
