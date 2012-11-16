import time
import string
import logging
from uuid import uuid4
from collections import namedtuple
from contextlib import contextmanager

import xapian
import nilsimsa

from operator import itemgetter
from functools import partial
from json import loads

from xapian import QueryParser, DocNotFoundError

from . import snowball
from .elements import Schema, String, Integer, Float, Array
from .exc import ValidationError, PrefixError
from .tools import LRUDict, lazy_property


RETRY_LIMIT = 5
RETRY_BACKOFF_FACTOR = 0.2

logger = logging.getLogger(__name__)

XAPIAN_VERSION = namedtuple('XAPIAN_VERSION', ['major', 'minor', 'revision'])

xapian_version = XAPIAN_VERSION(xapian.major_version(),
                                  xapian.minor_version(),
                                  xapian.revision())

default_parser_flags = (QueryParser.FLAG_PHRASE |
                        QueryParser.FLAG_BOOLEAN |
                        QueryParser.FLAG_LOVEHATE |
                        QueryParser.FLAG_SPELLING_CORRECTION |
                        QueryParser.FLAG_BOOLEAN_ANY_CASE |
                        QueryParser.FLAG_WILDCARD)


def _schema_name(schema):
    return "%s.%s" % (schema.__module__, schema.__name__)


def defaults(head, limit=0, mlimit=0, klimit=1.0, kmlimit=1.0):
    return (head, limit, mlimit, klimit, kmlimit)


def _lookup_schema(name):
    modname, expr = name.rsplit('.', 1)
    local_name = modname.split('.')[-1]
    mod = __import__(modname, {}, {}, local_name)
    return eval(expr, mod.__dict__)


def _prefix(name):
    return (u'X%s:' % name.upper()).encode('utf-8')


def to_term(value, prefix=None):
    value = value.lower()
    return _prefix(prefix) + value if prefix else value


class QuerySchema(Schema):
    type = String.using(default='xodbquery')
    term = Array.of(String).using(prefix='query_term',
                                  getter=lambda s, o, e: list(o))
    id = Array.of(String).using(prefix='query_id',
                                getter=lambda s, o, e: o.__xodb_id__)


class Query(object):
    __xodb_schema__ = QuerySchema

    class __metaclass__(type):

        def __getattr__(cls, name):
            return getattr(xapian.Query, name)

    def __init__(self, *args, **kwargs):
        self.__xodb_query__ = kwargs.pop('query', None) or xapian.Query(*args, **kwargs)
        self.__xodb_id__ = uuid4()

    def __getattr__(self, name):
        return getattr(self.__xodb_query__, name)

    def __getstate__(self):
        return dict(query=self.__xodb_query__.serialise(),
                    id=str(self.__xodb_id__))

    def __setstate__(self, state):
        self.__xodb_query__ = xapian_Query.unserialise(state['query'])
        self.__xodb_id__ = uuid.UUID(state['id'])

    def __str__(self):
        return str(self.__xodb_query__)

    def __iter__(self):
        return iter(self.__xodb_query__)


class RecordSchema(Schema):
    type = String.using(default='xodbrecord')
    rank = Integer.using(getter=lambda s, o, e: o._xodb_rank)
    percent = Integer.using(getter=lambda s, o, e: o._xodb_percent)
    weight = Integer.using(getter=lambda s, o, e: o._xodb_weight)


class Record(object):
    """Nice attribute-accessable record for a search result."""
    __xodb_schema__ = RecordSchema

    def __init__(self, document, percent, rank, weight, query, db):
        self._xodb_document = document
        self._xodb_percent = percent
        self._xodb_rank = rank
        self._xodb_weight = weight
        self._xodb_query = query
        self._xodb_db = db

    @lazy_property
    def _xodb_schema(self):
        typ, data = loads(self._xodb_document.get_data())
        return _lookup_schema(typ).from_flat(data)

    def __getattr__(self, name):
        try:
            return self._xodb_schema[name].value
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return repr(self._xodb_schema)

    def __getstate__(self):
        state = self.__dict__.copy()
        if '_xodb_schema' in state:
            del state['_xodb_schema']
        state['_xodb_document'] = self._xodb_document.serialise()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._xodb_document = xapian.Document.unserialise(self._xodb_document)


def record_factory(database, doc, percent, rank, weight, query, db):
    return Record(doc, percent, rank, weight, query, db)


class LanguageDecider(xapian.ExpandDecider):
    """
    A Xapian ExpandDecider that decide which terms to keep and which
    to discard when expanding a query using the "suggest" syntax.  As
    a place to start, we throw out:

      - Terms that don't begin with an uppercase letter or digit.
        This filters prefixed terms and stemmed forms.

      - Terms shorter than min_length chars, which are likely irrelevant

      - Stopwords for the given language.  Default is english, pass
        None for the language argument if no stopping is desired.
    """

    min_length = 5
    nostart = unicode(string.uppercase + string.digits)

    def __init__(self, language="en", filter=None, stems=None):
        super(LanguageDecider, self).__init__()
        if language in snowball.stoppers:
            self.stopper = snowball.stoppers[language]
            self.stemmer = xapian.Stem(language)
        else:
            self.stopper = lambda(term): False
            self.stemmer = xapian.Stem("none")
        self.stems = tuple(self.stemmer(t) for t in stems) if stems else ()

    def __call__(self, term):
        term = term.decode("utf-8")
        if (term[0] in self.nostart or
            len(term) < self.min_length or
            self.stopper(term) or
            '_' in term or
            self.stemmer(term) in self.stems):
            return False
        return True


class PrefixDecider(xapian.ExpandDecider):
    """Expand decider to only match terms that begin with a prefix."""

    __slots__ = ['prefix']

    def __init__(self, prefix):
        super(PrefixDecider, self).__init__()
        self.prefix = (u'X%s:' % prefix.upper()).encode('utf-8')

    def __call__(self, term):
        return term.startswith(self.prefix)


class MultipleValueRangeProcessor(xapian.ValueRangeProcessor):
    """Value range processor for multiple prefixes.

    :param map: a dict of prefix to value number pairs.

    :param serializer: optional callable to serialize the range arguments into
                       the same form as the corresponding values are stored.
                       Typically xapian.sortable_serialise for floats,
                       str.lower for strings.

    :param end_serializer: optional callable to serialize the end argument
                           of the range into the same form as the values.
                           Defaults to the value of *serializer*.

    """

    def __init__(self, map, serializer=None, end_serializer=None):
        self.map = map
        self.begin_serializer = serializer or (lambda x: x)
        self.end_serializer = end_serializer or self.begin_serializer
        xapian.ValueRangeProcessor.__init__(self)

    def __call__(self, begin, end):
        for prefix, value in self.map.items():
            if begin.startswith(prefix + ':'):
                return (value,
                        self.begin_serializer(begin[len(prefix) + 1:]),
                        self.end_serializer(end))
        return (xapian.BAD_VALUENO, begin, end)


class Database(object):
    """An xodb database.

    :param db_or_path: A path to file, or a xapian.Database object
    that backs this Database instance.

    :param writable: Open database in writable mode.

    :param overwrite: If writable is True, overwrite the existing
    database with a new one.

    :param spelling: If True, write spelling correction data to the
    database.

    :param replicated: If True, the database is opened read-only in
    replication mode (reopen() is never called, see xapian ticket
    #434)

    """

    record_factory = record_factory

    relevance_prefix = "_XODB_RP_"
    boolean_prefix = "_XODB_BP_"
    value_prefix = "_XODB_VALUE_"
    value_sort_prefix = "_XODB_VALUESORT_"
    value_count_name = "_XODB_COUNT_"
    backend = None
    _metadata_keyset = None
    query_cache_limit = 1024

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield self
            self.commit()
        except:
            self.cancel()
            raise

    def retry_if_modified(self, operation, limit=RETRY_LIMIT, refresh=True):
        tries = 0
        while True:
            try:
                return operation()
            except xapian.DatabaseError, e:
                if tries >= limit:
                    logger.warning(
                        '%s after %s retries, failing.', e, tries)
                    raise
                logger.info('%s: after %s retries, retrying', e, tries)
                time.sleep(tries * RETRY_BACKOFF_FACTOR)
                self.reopen(refresh_if_needed=refresh)
                tries += 1

    def __init__(self, path,
                 writable=True,
                 overwrite=False,
                 spelling=True,
                 replicated=False,
                 inmem=False):
        self.db_path = path
        self._writable = writable
        self._overwrite = overwrite
        self.spelling = spelling
        self.replicated = replicated
        self.type_map = {}
        self.parsers_by_language = {}
        self.relevance_prefixes = {}
        self.boolean_prefixes = {}
        self.values = {}
        self.value_sorts = {}
        self.query_cache = LRUDict(limit=self.query_cache_limit)
        self.inmem = inmem
        self._value_count = 0

        if isinstance(path, basestring):
            if writable:
                if replicated:
                    raise TypeError("replication cannot be used with "
                                    "writable databases")
                if overwrite:
                    flags = xapian.DB_CREATE_OR_OVERWRITE
                else:
                    flags = xapian.DB_CREATE_OR_OPEN
                self.backend = xapian.WritableDatabase(path, flags)
            else:
                self.backend = xapian.Database(path)
        elif isinstance(path, xapian.Database):
            if replicated:
                raise TypeError("replication can only be used "
                                "if a database path is provided.")
            self.backend = self.db_path

        self.reopen()

    def get_query_parser(self, language, default_op, check_cache=True, 
                         retry_limit=RETRY_LIMIT):
        qp = None
        if check_cache and not self.inmem:
            qp = self.parsers_by_language.get(language)
            if qp is not None:
                return qp
        def prepare_op():
            return self.prepare_query_parser(language, default_op)
        qp = self.retry_if_modified(prepare_op, retry_limit)
        if check_cache and not self.inmem:
            self.parsers_by_language[language] = qp
        return qp

    def close(self):
        self.backend.close()

    def flush(self):
        self.backend.flush()

    def map(self, otype, schema):
        """Map a type to a schema."""
        self.type_map[otype] = schema

    def schema_for(self, otype):
        """Get the schema for a given type, or one of its
        superclasses."""
        if hasattr(otype, '__xodb_schema__'):
            return otype.__xodb_schema__
        for base in otype.__mro__:
            if base in self.type_map:
                return self.type_map[base]
        raise TypeError("No schema defined for %s" % repr(otype))

    def _get_value_count(self):
        if self.inmem:
            return self._value_count
        return int(self.backend.get_metadata(self.value_count_name))

    def _set_value_count(self, count):
        if self.inmem:
            self._value_count = count
        else:
            self.backend.set_metadata(self.value_count_name, str(count))

    value_count = property(_get_value_count, _set_value_count)

    def check_prefix(self, name, boolean=False):
        prefixes = set(self.relevance_prefixes.keys())
        prefixes = prefixes.union(self.boolean_prefixes.keys())
        if name not in prefixes:
            upped = _prefix(name)
            if boolean:
                self.add_boolean_prefix(name, upped)
            else:
                self.add_prefix(name, upped)

    def add_prefix(self, key, value, ignore_duplicates=True):
        """Add a prefix mapping to the database.
        """
        # FIXME:
        # Why is it only checking boolean prefixes and then adding to relevance?
        if key in self.boolean_prefixes:
            if ignore_duplicates:
                logger.warning('Conflicting relevance prefix %s', key)
                return
            else:
                raise PrefixError('Conflicting relevance prefix %s', key)
        self.relevance_prefixes[key] = value
        self.backend.set_metadata(self.relevance_prefix + key, value)

    def add_boolean_prefix(self, key, value):
        """Add a boolean prefix mapping to the database.
        """
        self.boolean_prefixes[key] = value
        self.backend.set_metadata(self.boolean_prefix + key, value)

    def allocate_value_index(self, name):
        """Default implementation of value index number allocation.

        Implement your own using shared something to keep several
        databases in sync. i.e. for indexing in parallel.
        """
        value_count = self.value_count + 1
        self.value_count = value_count
        return value_count

    def add_value(self, name, sort=None):
        """Add a value mapping to the database.
        """
        if name in self.values:
            return self.values[name]
        value_index = self.allocate_value_index(name)
        self.values[name] = value_index
        self.backend.set_metadata(self.value_prefix + name, str(value_index))
        if sort:
            self.value_sorts[name] = sort
            self.backend.set_metadata(self.value_sort_prefix + name, sort)
        return value_index

    def __len__(self):
        """ Return the number of documents in this database. """
        self.reopen()
        return self.backend.get_doccount()

    def allterms(self, prefix="", retry_limit=RETRY_LIMIT):
        self.reopen()
        seen = set()
        tries = 0
        # we can't use retry_if_modified because this
        # is not an atomic operation that returns one result
        while True:
            try:
                for t in self.backend.allterms(prefix):
                    term = t.term
                    if term in seen:
                        continue
                    seen.add(term)
                    yield term
                break
            except xapian.DatabaseError:
                if tries > retry_limit:
                    logger.warning(
                        'allterms() failed after %s retries.',
                        tries)
                    raise
                logger.info('Retrying allterms() operation.')
                time.sleep(tries * .1)
                self.reopen()
                tries += 1

    def get(self, docid, default=None):
        """ Get a document with the given docid, or the default value if
        no such document exists.
        """
        try:
            return self[docid]
        except DocNotFoundError:
            return default

    def __getitem__(self, docid):
        return self.backend.get_document(docid)

    def __delitem__(self, docid):
        self.backend.delete_document(docid)

    def __setitem__(self, docid, document):
        doc = self.get(docid)
        if doc is None:
            self.backend.add_document(document)
        else:
            self.backend.replace_document(docid, document)

    def __contains__(self, docid):
        return True if self.get(docid) else False

    def add(self, *objs, **kw):
        """Add an object to the database by transforming it into a
        xapian document.  It's type or one of its base types must be
        mapped to a schema before an object can be added here.

        :params objs: One or more mapped objects to add.

        :param schema_type: Specify the schema to be used. (optional)

        :param validate: Validated the schema before the object is
        added.  Default: True

        Returns a list of xapan documents that were added to the
        database.
        """
        assert self._writable, "Database is not writable"
        added = []
        validate = kw.pop('validate', True)
        schema_type = kw.pop('schema_type', None)
        for obj in objs:
            if isinstance(obj, xapian.Document):
                self.backend.add_document(obj)
                added.append(obj)
                continue
            doc = self.to_document(obj, schema_type=schema_type,
                                   validate=validate)
            self.backend.add_document(doc)
            added.append(doc)
        return added

    def replace(self, obj, docid, **kw):
        """Add or replace an object in the database with a specified
        document id by transforming it into a xapian document.
        It's type or one of its base types must be
        mapped to a schema before an object can be added here.

        :param obj: The mapped objects to add.

        :param docid:  The document id to user for the mapped object.

        :param schema_type: Specify the schema to be used. (optional)

        :param validate: Validated the schema before the object is
        added.  Default: True

        Returns the xapan document that was added to the
        database.
        """
        assert self._writable, "Database is not writable"
        validate = kw.pop('validate', True)
        schema_type = kw.pop('schema_type', None)
        if not isinstance(obj, xapian.Document):
            doc = self.to_document(obj, schema_type=schema_type,
                                   validate=validate)
        else:
            doc = obj
        self.backend.replace_document(int(docid), doc)
        return doc

    def to_schema(self, obj, validate=True, schema_type=None):
        """
        Turn an object into an schema instance which is fully
        populated with data from the object.  Optionally validate.
        """
        if not schema_type:
            if hasattr(obj, '__xodb_schema__'):
                schema_type = obj.__xodb_schema__
            else:
                schema_type = self.schema_for(type(obj))
        schema = schema_type.from_defaults()
        schema.__xodb_db__ = self
        schema.update_by_object(obj)

        if validate and not schema.validate():
            invalid = []
            for child in schema.all_children:
                if not child.valid:
                    invalid.append(child)
            raise ValidationError("Elements of %s did not validate %s:" %
                                  (schema.__class__.__name__,
                                   list((c.name, c.value)
                                        for c in invalid)))
        return schema

    def to_document(self, obj, validate=True, schema_type=None):
        """
        Convienient wrapper that does the object->schema->document
        transformation.
        """
        if not isinstance(obj, Schema):
            obj = self.to_schema(obj, validate, schema_type)
        return self.doc_from_dict(obj.__xodb_memo__.dict)

    def doc_from_dict(self, data):
        """Take an intermediate representation of a document (a
        "memo") and turn it into a xapian document.  The document is
        returned and not added to the database.
        """
        doc = xapian.Document()

        for term_tup in data.get('terms', ()):
            term = None
            typ = None
            wdfinc = 1
            if len(term_tup) > 2:
                term, typ, wdfinc = term_tup
            else:
                term, typ = term_tup
            if ':' in term:
                prefix, _, value = term.partition(':')
                self.check_prefix(prefix, typ)
                term = 'X%s:%s' % (prefix.upper(), value)
            doc.add_term(term, wdfinc)
        for post in data.get('posts', ()):
            pass

        all_start_pos = 0
        for text_dict in data.get('texts', ()):
            lang = text_dict.get('lang')
            el_start_pos = text_dict.get('position_start', None)

            tg = xapian.TermGenerator()
            tg.set_database(self.backend)
            tg.set_document(doc)

            # if the element specifies no start position, set the
            # starting position where the last element left off
            if el_start_pos is not None:
                tg.set_termpos(el_start_pos)
            else:
                tg.set_termpos(all_start_pos)

            spelling = text_dict.get('spell', True)
            if spelling:
                try:
                    # hack to workaround missing spelling for inmem backends
                    self.backend.add_spelling('food')
                    tg.set_flags(xapian.TermGenerator.FLAG_SPELLING)
                except:
                    pass  # noop for backends that don't support spelling
            if lang in snowball.stoppers:
                tg.set_stemmer(xapian.Stem(lang))
                tg.set_stopper(snowball.stoppers[lang])
            if text_dict.get('post', True):
                index_text = tg.index_text
            else:
                index_text = tg.index_text_without_positions
            text = text_dict.get('text')
            if text:
                wdf_inc = text_dict.get('wdf_inc', 1)
                prefix = text_dict.get('prefix')
                if prefix:
                    self.check_prefix(prefix)
                    prefix = 'X%s:' % prefix.upper()
                    index_text(text, wdf_inc, prefix)
                else:
                    index_text(text, wdf_inc)
            # if the element specified no start position,
            # update the all-document position
            if el_start_pos is None:
                all_start_pos = tg.get_termpos()

        for val_tuple in data.get('values', ()):
            name, value, typ = val_tuple
            if name in self.values:
                valno = self.values[name]
            else:
                valno = self.add_value(name, typ)
            if typ in ('integer',):
                value = xapian.sortable_serialise(value)
            doc.add_value(valno, value)

        data = data.get('data')
        if data:
            doc.set_data(data)
        return doc

    def reopen(self, retry_limit=RETRY_LIMIT, refresh_if_needed=True):
        """
        Reopen the database.  Called before most query methods.  If
        replication is used, the db is closed and reopened.
        """
        if not self.replicated:
            self.backend.reopen()
        else:
            # replication does not support the reopen() method, so the
            # db must be explicitly closed and reopened.
            assert self.db_path, ("Must provide a db path when "
                                  "using replication.")
            self.close()
            self.backend = xapian.Database(self.db_path)
            # reset cached parsers to new database object
            for parser in self.parsers_by_language.itervalues():
                parser.set_database(self.backend)

        if refresh_if_needed and self.is_metadata_changed:
            self.meta_refresh()

    def begin(self):
        if self._writable:
            self.reopen()
            try:
                self.backend.begin_transaction()
            except Exception:
                pass  # noop for backends that don't support transactions

    def cancel(self):
        if self._writable:
            try:
                self.backend.cancel_transaction()
            except Exception:
                pass

    def commit(self):
        if self._writable:
            try:
                self.backend.commit_transaction()
            except Exception:
                pass

    def prepare_query_parser(self, language=None,
                             default_op=Query.OP_AND):
        """
        Setup a query parser with the current known prefixes and values.
        """
        qp = QueryParser()
        qp.set_database(self.backend)
        qp.set_default_op(default_op)
        
        if self.boolean_prefixes:
            for key, value in self.boolean_prefixes.items():
                qp.add_boolean_prefix(key, value)
        if self.relevance_prefixes:
            for key, value in self.relevance_prefixes.items():
                if key not in self.boolean_prefixes:
                    qp.add_prefix(key, value)
                else:
                    logger.warning(
                        'Duplicate relevance prefix %s conflicts with boolean',
                        key)
        if self.value_sorts:
            # First add numeric values ranges
            qp.add_valuerangeprocessor(MultipleValueRangeProcessor(
                dict(((k, self.values[k])
                      for k, v in self.value_sorts.items() if v == 'integer')),
                lambda s: xapian.sortable_serialise(float(s)),
            ))
            # Then string and date
            qp.add_valuerangeprocessor(MultipleValueRangeProcessor(
                dict(((k, self.values[k])
                      for k, v in self.value_sorts.items() if v in
                        ('string', 'date'))),
            ))
            # Serialize date range queries so that they are inclusive.
            # This allows datetime value range queries to be treated
            # as [begin,end] rather than [begin,end) as is the default
            # without these serializers when then datetime range arguments
            # are not fully qualified.
            qp.add_valuerangeprocessor(MultipleValueRangeProcessor(
                dict(((k, self.values[k])
                      for k, v in self.value_sorts.items() if v == 'datetime')),
                serializer = lambda x: x + '0'*(14-len(x)),
                end_serializer = lambda x: x + '9'*(14-len(x))
            ))
        if language in snowball.stoppers:
            qp.set_stemmer(xapian.Stem(language))
            qp.set_stopper(snowball.stoppers[language])
            qp.set_stemming_strategy(QueryParser.STEM_SOME)
        return qp

    @property
    def is_metadata_changed(self):
        return self._metadata_keyset != self._get_metadata_keyset()

    def _get_metadata_keyset(self, retry_limit=RETRY_LIMIT):
        if self.inmem:
            return False
        op = lambda: set(self.backend.metadata_keys())
        # don't recurse into refresh here, just reopen and retry
        return self.retry_if_modified(op, retry_limit, False)

    def meta_refresh(self, retry_limit=RETRY_LIMIT):
        if not self.inmem:
            self.parsers_by_language = {}
            self.relevance_prefixes = {}
            self.boolean_prefixes = {}
            self.values = {}
            self.value_sorts = {}
            self.query_cache = LRUDict(limit=self.query_cache_limit)

            self._metadata_keyset = self._get_metadata_keyset()
            for k in self._metadata_keyset:
                op = lambda: self.backend.get_metadata(k)
                val = self.retry_if_modified(op, retry_limit, False)
                if k.startswith(self.relevance_prefix):
                    prefix = k[len(self.relevance_prefix):]
                    self.relevance_prefixes[prefix] = val
                elif k.startswith(self.boolean_prefix):
                    prefix = k[len(self.boolean_prefix):]
                    self.boolean_prefixes[prefix] = val
                elif k.startswith(self.value_prefix):
                    value = k[len(self.value_prefix):]
                    self.values[value] = int(val)
                elif k.startswith(self.value_sort_prefix):
                    value = k[len(self.value_sort_prefix):]
                    self.value_sorts[value] = val
            try:
                # hit the property to refresh this value
                count = self.value_count
                count = count  # pyflakes
            except ValueError:
                if self._writable:
                    self.value_count = 0

    def querify(self, query,
                language=None,
                translit=None,
                default_op=Query.OP_AND,
                parser_flags=default_parser_flags,
                retry_limit=RETRY_LIMIT):
        """Return a query object, constructed from a string, query
        object, or mixed sequence of any number of strings, query
        objects, or subsequences of the same form.

        If 'query' is a xapian query object, it is returned unchanged.
        If it's a string, it is parsed with xapian's query parser and
        returned.  If it is a sequence, a new query is constructed
        with the 'default_op' operator and the sequence is iterated
        into the new query, recursively querify on each item of the
        sequence.
        """
        if isinstance(query, Query):
            return query
        if isinstance(query, basestring):
            if query == "":
                return Query("")
            else:
                cache_key = (query, language, translit, default_op, parser_flags)
                if cache_key in self.query_cache:
                    return self.query_cache[cache_key]
                query = query.lower()
                if translit:
                    query = query.encode(translit)

                qp = self.get_query_parser(language, default_op, 
                                           retry_limit=retry_limit)
                def query_op():
                    return Query(query=qp.parse_query(query, parser_flags))
                result = self.retry_if_modified(query_op, retry_limit)
                if not self.inmem:
                    self.query_cache[cache_key] = result
                return result
        else:
            return reduce(partial(Query, default_op),
                          (self.querify(q,
                                        language,
                                        translit,
                                        default_op,
                                        parser_flags) for q in query))

    def query(self, query,
              offset=0,
              limit=0,
              order=None,
              reverse=False,
              language=None,
              check=0,
              translit=None,
              match_decider=None,
              match_spy=None,
              document=False,
              echo=False,
              disimilate=False,
              disimilate_field='nilsimsa',
              disimilate_threshold=100,
              parser_flags=default_parser_flags,
              default_op=Query.OP_AND,
              retry_limit=RETRY_LIMIT):
        """
        Query the database with the provided string or xapian Query
        object.  A string is passed into xapians QueryParser first to
        generate a Query object.
        """
        self.reopen()

        enq = xapian.Enquire(self.backend)
        if echo:
            print "Parsing query."
        query = self.querify(query, language, translit,
                             default_op, parser_flags)
        if echo:
            print "Done parsing query: %s" % str(query)
        enq.set_query(query.__xodb_query__)

        limit = limit or self.backend.get_doccount()

        if echo:
            start = time.time()
            print "Fetching mset..."

        # convoluted logic here is to retry queries that die in the
        # middle of result iteration because the db was closed (due to
        # replication).

        tries = 0
        seen = set()
        disimilator = set()
        sim_comp = nilsimsa.compare_hexdigests
        while True:
            try:
                # _build_mset may retry internally on DatabaseError
                mset = self._build_mset(enq, offset, limit, order, reverse,
                                        check, match_decider, match_spy,
                                        retry_limit=retry_limit)
                if echo:
                    print "Fetched mset in %s" % str(time.time() - start)

                for record in mset:
                    doc = record.document
                    docid = doc.get_docid()
                    if docid in seen:
                        continue
                    if document:
                        seen.add(docid)
                        yield doc
                    else:
                        # retry getting the actual document data
                        op = lambda: self.backend.get_document(docid).get_data()
                        try:
                            data = self.retry_if_modified(op, retry_limit)
                        except DocNotFoundError:
                            logger.warning(
                                "Document %d is gone, skipping.", docid)
                            continue
                        typ, data = loads(data)
                        seen.add(docid)
                        record = self.record_factory(doc,
                                                     record.percent,
                                                     record.rank,
                                                     record.weight,
                                                     query,
                                                     self,
                                                     )
                        if disimilate:
                            yield_it = True
                            rhash = getattr(record, disimilate_field, None)
                            if rhash:
                                if any((sim_comp(rhash, h) > disimilate_threshold)
                                       for h in set(disimilator)):
                                    yield_it = False
                            if yield_it:
                                disimilator.add(rhash)
                                yield record
                        else:
                            yield record
                # no errors exhuasting the set? break out and we're done
                break
            except xapian.DatabaseError, e:
                # an error occured, either, the db was closed, or the
                # modified error happened two frequently in the inner
                # loop, so we are going to replay the whole query
                if tries > retry_limit:
                    logger.warning(
                        'Database replay failed after %s retries.',
                        tries)
                    raise
                logger.info('Replaying database query.')
                self.reopen()
                tries += 1

    def count(self,
              query="",
              language=None,
              echo=False,
              translit=None,
              parser_flags=default_parser_flags,
              default_op=Query.OP_AND,
              retry_limit=RETRY_LIMIT):
        """
        Query the database with the provided string or xapian Query
        object.  A string is passed into xapians QueryParser first to
        generate a Query object.
        """
        self.reopen()
        query = self.querify(query, language, translit, default_op, parser_flags)
        if echo:
            print str(query)
        enq = xapian.Enquire(self.backend)
        enq.set_query(query.__xodb_query__)

        mset = self._build_mset(enq, retry_limit=retry_limit)
        return mset.size()

    def facet(self, query,
              prefix='facet',
              estimate=True,
              language=None,
              limit=0,
              mlimit=0,
              klimit=1.0,
              kmlimit=1.0,
              echo=False,
              retry_limit=RETRY_LIMIT):
        """Get facet suggestions for the query, then the query with
        each suggested facet, asking xapian for an estimated count of
        each sub-query.
        """
        if estimate:
            counter = self.estimate
        else:
            counter = self.count

        results = {}
        query = self.querify(query, language=language)

        suggestions = self.suggest(query,
                                   prefix=prefix,
                                   language=language,
                                   limit=limit,
                                   mlimit=mlimit,
                                   klimit=klimit,
                                   kmlimit=kmlimit,
                                   echo=echo,
                                   retry_limit=retry_limit,
                                   format_term=False)
        for facet in suggestions:
            q = Query(Query.OP_AND, [query, facet])
            if echo:
                print str(q)
            if prefix and facet.startswith('X%s:' % prefix.upper()):
                suffix = facet[len(prefix) + 2:]
                if ' ' in suffix or '..' in suffix:
                    facet = '%s:"%s"' % (prefix, suffix)
                else:
                    facet = '%s:%s' % (prefix, suffix)
            results[facet] = counter(q, language=language)
        return results

    def expand(self, query, expand,
               language=None,
               echo=False,
               translit=None,
               default_op=Query.OP_AND,
               parser_flags=default_parser_flags,
               retry_limit=RETRY_LIMIT):
        """
        Expand a query on a given set of prefixes.
        """

        q = self.querify(query, language, translit, default_op, parser_flags,
                         retry_limit=retry_limit)
        if echo:
            print q
        results = {}
        head, tail = expand[0], expand[1:]
        if isinstance(head, tuple):
            args = head
        else:
            args = (head,)
        head, limit, mlimit, klimit, kmlimit = defaults(*args)

        r = self.facet(
            q, prefix=head, language=language, echo=echo,
            limit=limit, mlimit=mlimit,
            klimit=klimit, kmlimit=kmlimit,
            retry_limit=retry_limit).items()

        for name, score in r:
            if tail:
                subq = self.querify([q, name], retry_limit=retry_limit)
                if echo:
                    print subq
                r = self.expand(
                    subq, tail, language, echo,
                    default_op, parser_flags,
                    retry_limit=retry_limit)
                results[(name, score)] = r
            else:
                results[(name, score)] = score
        return results

    def estimate(self, query,
                 limit=0,
                 klimit=1.0,
                 language=None,
                 translit=None,
                 default_op=Query.OP_AND,
                 parser_flags=default_parser_flags,
                 retry_limit=RETRY_LIMIT):
        """Estimate the number of documents that will be yielded with the
        given query.

        Limit tells the estimator the minimum number of documents to
        consider.  A zero limit means potentially check all documents
        in the db."""
        self.reopen()
        enq = xapian.Enquire(self.backend)

        if limit == 0:
            limit = int(self.backend.get_doccount() * klimit)

        query = self.querify(query, language, translit,
                             default_op, parser_flags,
                             retry_limit=retry_limit)

        enq.set_query(query.__xodb_query__)
        op = lambda: enq.get_mset(0, 0, limit)
        mset = self.retry_if_modified(op, retry_limit)

        return mset.get_matches_estimated()

    def term_freq(self, term):
        """
        Return a count of the number of documents indexed for a given
        term.  Useful for testing.
        """
        self.reopen()
        return self.backend.get_termfreq(term)

    def describe_query(self, query,
                       language=None,
                       default_op=Query.OP_AND,
                       retry_limit=RETRY_LIMIT):
        """
        Describe the parsed query.
        """
        qp = self.get_query_parser(language, default_op,
                                   retry_limit=RETRY_LIMIT)
        def op():
            return qp.parse_query(query, default_parser_flags) 
        return str(self.retry_if_modified(op, retry_limit))

    def spell(self, query,
              language=None,
              default_op=Query.OP_AND,
              retry_limit=RETRY_LIMIT):
        """
        Suggest a query string with corrected spelling.
        """
        self.reopen()
        qp = self.get_query_parser(language, default_op, 
                                   retry_limit=RETRY_LIMIT)
        def op():
            qp.parse_query(query, QueryParser.FLAG_SPELLING_CORRECTION)
            return qp.get_corrected_query_string().decode('utf8')
        return self.retry_if_modified(op, retry_limit)

    def suggest(self, query,
                offset=0,
                limit=0,
                moffset=0,
                mlimit=0,
                klimit=1.0,
                kmlimit=1.0,
                translit=None,
                language=None,
                prefix=None,
                decider=None,
                score=False,
                echo=False,
                default_op=Query.OP_AND,
                parser_flags=default_parser_flags,
                retry_limit=RETRY_LIMIT,
                format_term=True,
                collapse_stems=True):
        """
        Suggest terms that would possibly yield more relevant results
        for the given query.
        """
        self.reopen()
        enq = xapian.Enquire(self.backend)

        query = self.querify(query, language, translit, default_op, parser_flags)

        if mlimit == 0:
            mlimit = int(self.backend.get_doccount() * kmlimit)

        if echo:
            print str(query)
        enq.set_query(query.__xodb_query__)

        mset = self._build_mset(enq, offset=moffset, limit=mlimit,
                                retry_limit=retry_limit)

        rset = xapian.RSet()
        for m in mset:
            rset.add_document(m.docid)

        if prefix is not None:
            decider = PrefixDecider(prefix)

        if decider is None:
            decider = LanguageDecider(language)

        if limit == 0:
            limit = int(self.backend.get_doccount() * klimit)

        if xapian_version <= (1, 2, 4):
            op = lambda: enq.get_eset(
                limit, rset,
                enq.INCLUDE_QUERY_TERMS,
                1.0, decider)
        else:
            op = lambda: enq.get_eset(
                limit, rset,
                enq.INCLUDE_QUERY_TERMS,
                1.0, decider, -3)

        stemmer = None
        stems = None
        if collapse_stems:
            stems = set()
            stemmer = xapian.Stem(language)

        eset = self.retry_if_modified(op, retry_limit)

        for item in eset.items:
            val = item[0].decode('utf8')
            if format_term and prefix and val.startswith('X%s:' % prefix.upper()):
                suffix = val[len(prefix) + 2:]
                if ' ' in suffix or '..' in suffix:
                    val = '%s:"%s"' % (prefix, suffix)
                else:
                    val = '%s:%s' % (prefix, suffix)
            if collapse_stems:
                if stemmer(val) in stems:
                    continue
                stems.add(stemmer(val))
            if score:
                yield (val, item[1])
            else:
                yield val

    def _build_mset(self, enq,
                    offset=0,
                    limit=None,
                    order=None,
                    reverse=False,
                    check=None,
                    match_decider=None,
                    match_spy=None,
                    retry_limit=RETRY_LIMIT):
        if order is not None:
            if isinstance(order, basestring):
                try:
                    order = self.values[order]
                except KeyError:
                    raise ValueError("There is no sort name %s" % order)
            enq.set_sort_by_value(order, reverse)

        if limit is None:
            limit = self.backend.get_doccount()

        if check is None:
            check = limit + 1

        op = lambda: enq.get_mset(
                offset, limit, check, None, match_decider, match_spy)
        return self.retry_if_modified(op, retry_limit)


def jsonrpc_wrapper(f):
    def w(database, *args, **kw):
        # unstupidify the way the jsonrpc spec calls us
        if not args:
            args = []
            for k in kw:
                try:
                    i = int(k)
                except ValueError:
                    continue
                args.append((i, kw.pop(k)))
            args.sort(key=itemgetter(0))
            args = tuple(a[1] for a in args)
        return f(database, *args, **kw)
    return w


class JSONDatabase(object):
    """
    Thunk layer on top of database that returns json data.
    """

    def jsonify_record(database, record):
        return record.flatten()

    def __init__(self, database):
        database.record_factory = self.jsonify_record
        self._database = database

    @jsonrpc_wrapper
    def query(self, *args, **kw):
        return list(self._database.query(*args, **kw))

    @jsonrpc_wrapper
    def suggest(self, *args, **kw):
        return list(self._database.query(*args, **kw))

    @jsonrpc_wrapper
    def facet(self, *args, **kw):
        return self._database.facet(*args, **kw)

    @jsonrpc_wrapper
    def expand(self, *args, **kw):
        return self._database.expand(*args, **kw)

    @jsonrpc_wrapper
    def count(self, *args, **kw):
        return self._database.count(*args, **kw)
