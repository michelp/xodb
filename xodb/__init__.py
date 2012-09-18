from __future__ import absolute_import

import tempfile
import xapian

from . tools import geoprint

from . database import (
    Database,
    JSONDatabase,
    LanguageDecider,
    MultipleValueRangeProcessor,
    )

from . elements import (
    Array,
    Date,
    DateTime,
    Dict,
    Integer,
    List,
    Location,
    NumericRange,
    Schema,
    String,
    Text,
    )

__all__ = [
    'Array',
    'Database',
    'Date',
    'DateTime',
    'Dict',
    'Integer',
    'JSONDatabase',
    'LanguageDecider',
    'List',
    'Location',
    'MultipleValueRangeProcessor',
    'NumericRange',
    'Schema',
    'String',
    'Text',
    'geoprint',
    'inmemory',
    'open',
    'temp',
    ]


def open(path_or_db,
         writable=True,
         overwrite=False,
         spelling=True,
         replicated=False,
         inmem=False):
    """Return an xodb database with the given path or xapian database object.

    :param path_or_db: A path to a database file or a pre-existing
    xapian database object.

    :param writable: Open database in writable mode.

    :param overwrite: If writable is True, overwrite the existing
    database with a new one.

    :param spelling: If True, write spelling correction data to the
    database.
    """
    return Database(path_or_db,
                    writable=writable,
                    overwrite=overwrite,
                    spelling=spelling,
                    replicated=replicated,
                    inmem=inmem)


def temp(spelling=True):
    """Returns an xodb database backed by a teporary directory.  You
    are responsible for cleaning up the directory.

    :param spelling: If True, write spelling correction data to database.
    """
    return open(tempfile.mkdtemp(), spelling=spelling)


def inmemory():
    """Returns an xodb database backed by an in-memory xapian
    database.  Does not support spelling correction.
    """
    return open(xapian.inmemory_open(), spelling=False, inmem=True)
