import logging
import unicodedata
import translitcodec

import nilsimsa
from flatland import schema
from flatland.schema.forms import _MetaForm
from json import dumps

from . memo import Memo
from . tools import geoprint

from .exc import (
    InvalidTermError,
    )

logger = logging.getLogger(__name__)

def _schema_name(schema):
    return "%s.%s" % (schema.__module__, schema.__class__.__name__)


def _prefix(prefix, value=None):
    if prefix is None:
        return
    return ("%s:%s" % (prefix, value)) if value else prefix


def _normalize(value):
    return unicodedata.normalize(
        'NFKC', unicode(value).strip().lower()).encode('utf-8')


_use_schema = object() # marker says use schema term generator


class SparseForm(schema.SparseDict):
    __metaclass__ = _MetaForm

    minimum_fields = 'required'


class Schema(SparseForm):

    language = None
    """The overarching language for this schema.

    If None, no language features are applied to Text elements unless
    they explicitly provide their own language.

    If a two letter language code, is used as the language for Text
    elements in this schema unless they specifically provide their
    own.
    """

    facet_prefix = 'facet'
    """The default prefix that will be used for storing and searching
    for facets.
    """

    ignore_invalid_terms = False
    """When True, invalid terms will be skipped with a warning.
    When False (default) an invalid term in a document raises InvalidTermError.
    """

    __xodb_db__ = None

    def update_by_object(self, obj):
        """Updates fields with an object's attributes.

        :param obj: any object

        Sets fields on *self*, using as many attributes as possible from
        *obj*.  Object attributes that do not correspond to field names are
        ignored.
        """
        for schema in self.field_schema:
            name = schema.name
            element = schema.from_defaults()
            self[name] = element.value
            value = None
            if element.getter is not None:
                value = element.getter(self, obj, element)
            elif hasattr(obj, name):
                value = getattr(obj, name)
            if value is None and not element.is_empty:
                if element.optional:
                    del self[name]
            else:
                self[name] = value

    @property
    def __xodb_memo__(self):
        """
        Walk the children, indexing each one with a handler into a
        memo object.
        """
        lang = None
        try:
            root_lang = self.root.get('language')
            lang = root_lang.value or root_lang.default
        except KeyError:
            lang = self.language

        self._memo = Memo()
        self._memo.set_lang(lang)
        self._handle_children(self, None)
        self._memo.data = dumps((_schema_name(self), self.flatten()))
        return self._memo

    def _handle_children(self, parent, grandparent):
        for el in parent.children:
            if el.index:
                if isinstance(el, String):
                    handler = self._handle_string
                elif isinstance(el, Integer):
                    handler = self._handle_integer
                elif isinstance(el, Float):
                    handler = self._handle_float
                elif isinstance(el, Boolean):
                    handler = self._handle_boolean
                elif isinstance(el, Date):
                    handler = self._handle_date
                elif isinstance(el, DateTime):
                    handler = self._handle_datetime
                elif isinstance(el, Text):
                    handler = self._handle_text
                elif isinstance(el, NumericRange):
                    handler = self._handle_numericrange
                elif isinstance(el, Location):
                    handler = self._handle_location
                elif isinstance(el, Nilsimsa):
                    handler = self._handle_nilsimsa
                elif isinstance(el, List):
                    handler = self._handle_children
                elif isinstance(el, Dict):
                    handler = self._handle_children
                elif isinstance(el, Array):
                    handler = self._handle_children
                else:
                    raise TypeError("Unknown element %s" % el)

                value = None
                try:
                    value = handler(el, parent)
                except InvalidTermError, e:
                    if self.ignore_invalid_terms:
                        logger.warning('Invalid term ignored: %r' % el)
                    else:
                        raise

                if value is not None:
                    if el.facet:
                        if el.name:
                            term = el.name.lower()
                            self._memo.add_term(
                                _prefix(self.facet_prefix, term),
                                True)
            if not el.store:
                el.value = None
        return True

    def _handle_scalar(self, term, value, element, type=None):
        name = element.flattened_name()
        memo = self._memo
        if term:
            if element.lower:
                term = term.lower()
            if element.prefix:
                memo.add_term(_prefix(name, term), element.boolean, element.wdf_inc)
            else:
                memo.add_term(term, element.boolean, element.wdf_inc)
            if element.sortable:
                memo.add_value(name, value, type)
            return value

    def _handle_string(self, element, parent):
        term = element.u
        value = element.value
        if value:
            return self._handle_scalar(term, value, element, 'string')

    def _handle_integer(self, element, parent):
        term = element.u
        value = element.value
        if value:
            return self._handle_scalar(term, value, element, 'integer')

    def _handle_float(self, element, parent):
        # TODO:mp floats are currently storage only
        pass

    def _handle_boolean(self, element, parent):
        value = 'true' if element.value else 'false'
        if value:
            return self._handle_scalar(value, value, element, 'integer')

    def _handle_date(self, element, parent):
        if element.value:
            term = element.value.strftime(element.term_format)
            value = element.value.strftime(element.value_format)
            return self._handle_scalar(term, value, element, 'date')

    def _handle_datetime(self, element, parent):
        if element.value:
            term = element.value.strftime(element.term_format)
            value = element.value.strftime(element.value_format)
            return self._handle_scalar(term, value, element, 'datetime')

    def _handle_text(self, element, parent):
        value = element.value
        if value is None:
            return
        name = element.flattened_name()
        memo = self._memo

        if element.language is _use_schema:
            lang = memo.lang
        else:
            lang = element.language

        if element.translit:
            value = value.encode(element.translit)

        if element.string:
            if value:
                if element.lower:
                    term = value.lower()
                if element.string_prefix:
                    memo.add_term(_prefix(element.string_prefix, term),
                                   element.boolean, element.wdf_inc)
                else:
                    memo.add_term(term, False, element.wdf_inc)

        if element.sortable:
            memo.add_value(name, value, 'string')

        value = _normalize(value)
        prefix = None
        if element.prefix:
            prefix = _prefix(name)
        memo.add_text(value, prefix, lang,
                      element.positions,
                      element.stem,
                      element.stop,
                      element.spelling,
                      element.wdf_inc,
                      element.position_start)
        return value

    def _handle_numericrange(self, element, parent):
        maxv = element['high'].value or 0
        minv = element['low'].value or 0
        if minv == maxv == 0:
            return

        step = element.step

        if minv < step:
            minv = 0
        else:
            minv = ((minv / step) * step)

        maxv = (((maxv + step) / step) * step)

        for i in xrange(minv, maxv, step):
            val = "%s_%s" % (i, i + step)
            val = val.lower()
            self._memo.add_term(_prefix(element.name, val), True, 
                                element.wdf_inc)
        return True

    def _handle_location(self, element, parent):
        memo = self._memo
        h = 'loc_' + element.hash(element.radians)
        memo.add_term(h, element.boolean, element.wdf_inc)
        if element.sortable:
            memo.add_value(element.name, h, 'location')
        return True

    def _handle_nilsimsa(self, element, parent):
        memo = self._memo
        try:
            val = (parent[element.from_field].value
                   .encode('translit/long')
                   .encode('ascii', 'ignore'))
            element.u = nilsimsa.Nilsimsa([val]).hexdigest()
        except Exception:
            print 'whoops: ', val
            return
        if element.u:
            return self._handle_scalar(element.u, element.u, element, 'string')
        return True


class _BaseElement(object):

    index = True
    """If True, generate terms and values for this element.

    If False, store the flattened element in the document record, but
    do not generate terms or values.
    """

    store = True
    """If True, store this elements value when the schema is saved.

    If False, this element is not stored on the schema.
    """

    getter = None
    """Getter function to calculate value from a given object.

    Use by Schema.update_by_object(obj) to overide the default getattr
    behavior and allow a schema author to provide custom logic.  If
    None, a getattr policy is used.

    Getter must have a two argument signature for (schema, obj).  The
    property decorator can be used to decorate a method that becomes
    this attribute.
    """

    prefix = True
    """If True, generate a prefix from the flattened name of the element.

    If False, the element is indexed with no prefix.
    """

    boolean = True
    """If True, then tell Xapian to treat this field as a boolean, not
    a probablistic term.

    Most elements default as boolean, text elements do not.
    """

    facet = False
    """Whether to generate facet terms for this elemement, or not.

    If True, the term 'facet:name' will be generated with the elements
    flattened name and added along with the elements other terms.

    If False, no faceting term is generated.
    """

    sortable = False
    """Whether to sort on the element or not.

    If True, a value slot in the document will be used to store the
    sortable value.
    """

    lower = True
    """Whether to lowercase the value when indexing the element's term.
    """

    wdf_inc = 1
    """How much to increment the within document frequency for terms
    generated by this element.
    """

    @classmethod
    def property(cls, fn):
        """Decorator for wrapping a schema class method getter with an
        element.

        Wrapped method must have the same signature expected by
        'getter'.
        """
        cls.getter = staticmethod(fn)
        return cls


class Boolean(schema.Boolean, _BaseElement):
    pass


class List(schema.List, _BaseElement):
    pass


class Dict(schema.Dict, _BaseElement):
    pass


class Array(schema.Array, _BaseElement):
    pass


class String(schema.String, _BaseElement):
    pass


class Integer(schema.Integer, _BaseElement):
    pass


# TODO:mp, float are currently storage only
class Float(schema.Float, _BaseElement):
    pass


class Date(schema.Date, _BaseElement):

    term_format = '%Y%m%d'
    """Format for rendering terms for this date.
    """

    value_format = '%Y%m%d'
    """Format for rendering value for this date.
    """


class DateTime(schema.DateTime, _BaseElement):

    term_format = '%Y%m%d'
    """Format for rendering terms for this date.
    """

    value_format = '%Y%m%d%H%M%S'
    """Format for rendering value for this date.
    """


class Text(schema.String, _BaseElement):

    boolean = False
    """If True, then tell Xapian to treat this field as a boolean, not
    a probablistic term.

    Most elements default as boolean, text elements do not.
    """

    language = _use_schema
    """The two letter code of the text element's language.

    If None, no language features will be applied at indexing time.

    If not None, will be used for this element, overriding the root
    schema language.

    The default is to inherit the language of the root schema.
    """

    positions = True
    """Tells the term generator to generate positional information for
    the text, or not.

    Positional information allows for phrase searching at the expense
    of a larger database.
    """

    position_start = None
    """Tells the indexer which position to start at when indexing text.

    If None, position is automatically determined on a per-document
    basis so that multiple text fields to *not* overlap.
    """

    stem = True
    """If the language is set, stem the text.
    """

    stop = True
    """If the language is set, remove stopwords from text.
    """

    spelling = True
    """True if the text should be added to the db as spelling correction targets.
    """

    string_prefix = None
    """If not None, defines the prefix that should be used for string
    indexing the text's value.

    This is useful to provide a different prefix than the name of the
    element, so that both the atomic value of the element and the
    stemmed textual value of the element can be indexed.
    """

    string = False
    """If true, also treat this field like a string.

    The elements value can also be indexed atomically with not
    language features applied.  Useful in conjunction with text_prefix
    to provide two prefixes for the text and atomic terms.
    """

    translit = 'translit/long'
    """Translit codec used to transform text terms into accent
    normalized forms.
    """


class _BaseRange(schema.Compound, _BaseElement):
    child_cls = None
    
    def __compound_init__(cls):
        cls.field_schema = [
            cls.child_cls.using(name='low',
                                optional=cls.optional),
            cls.child_cls.using(name='high',
                                optional=cls.optional),
            ]

    def compose(self):
        """Emits a tuple of low and high integers."""
        numbers = (self['low'].value, self['high'].value)
        display = ' to '.join(str(n) for n in numbers)
        return display, numbers

    def explode(self, value):
        """Consumes a sequence of low and high integers."""
        self['low'] = value[0]
        self['high'] = value[1]


class NumericRange(_BaseRange):

    child_cls = Integer

    step = 1
    """The step size for the numeric range.  Default is 1.
    """


class Nilsimsa(schema.String, _BaseElement):

    from_field = None
    """What other field the simhash should be computed from.

    If this is not provided, indexing will raise an error.
    """


class Location(schema.Compound, _BaseElement):
    """ Compound location is a 2-tuple of lat/lon coordinates.
    """

    radians = False
    """Presume elements coordinates are in radians.
    """
    
    child_cls = Float
    
    def __compound_init__(cls):
        cls.field_schema = [
            cls.child_cls.using(name='lat',
                                optional=cls.optional),
            cls.child_cls.using(name='lon',
                                optional=cls.optional),
            ]

    def compose(self):
        numbers = (self['lat'].value, self['lon'].value)
        display = ' by '.join(str(n) for n in numbers)
        return numbers, display

    def explode(self, value):
        self['lat'] = value[0]
        self['lon'] = value[1]

    def hash(self, radians=False):
        lat = self['lat'].value
        lon = self['lon'].value
        return geoprint.encode(lat, lon, radians=radians)
