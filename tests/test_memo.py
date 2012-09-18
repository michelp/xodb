import datetime
from json import dumps, loads
from nose.tools import assert_raises

from xodb.elements import (
    Array,
    Date,
    DateTime,
    Dict,
    Integer,
    List,
    Location,
    Schema,
    String,
    Text,
    )

from xodb.memo import Memo
from xodb.exc import InvalidTermError

class Object(object):

    def __init__(self, **kw):
        self.__dict__ = kw


class Stringer(Schema):
    s1 = String.named('s1')
    s2 = String.using(prefix=False)
    s3 = String.using(facet=True)
    s4 = String.using(boolean=False)
    s5 = String.using(sortable=True)
    s6 = String.using(default='foo')
    s7 = String.using(lower=False)
    s8 = String.using(wdf_inc=2)


def test_string():

    s = Stringer.from_defaults()
    f = Object(s1='s1',
               s2='s2',
               s3='s3',
               s4='s4',
               s5='S5',
               s7="WoZeRoD",
               s8="s8",
               )

    s.update_by_object(f)
    d = s.memo.dict
    assert d['lang'] == None
    assert (set(d['terms']) ==
            set([(u's3:s3', 'b', 1), (u'facet:s3', 'b'), (u's2', 'b', 1),
                 (u's1:s1', 'b', 1), (u's6:foo', 'b', 1), (u's5:s5', 'b', 1),
                 (u's4:s4', 'r', 1), (u's7:WoZeRoD', 'b', 1),(u's8:s8', 'b', 2)
                 ],
                )
            )
    assert set(d['values']) == set([(u's5', u'S5', 'string')])
    assert d['texts'] == []
    assert d['posts'] == []

    name, gots = loads(d['data'])
    gots = [tuple(i) for i in gots]
    assert name == "tests.test_memo.Stringer"
    assert set(gots) == set([("s3", "s3"), ("s2", "s2"), ("s1", "s1"),
                             ("s6", "foo"), ("s5", "S5"), ("s4", "s4"),
                             (u's7', u'WoZeRoD'), (u's8', 's8')])


class Inter(Schema):
    i1 = Integer.named('i1')
    i2 = Integer.using(prefix=False)
    i3 = Integer.using(facet=True)
    i4 = Integer.using(boolean=False)
    i5 = Integer.using(sortable=True)
    i6 = Integer.using(default=42)
    i7 = Integer.using(wdf_inc=2)


def test_integer():

    s = Inter.from_defaults()
    f = Object(i1=1,
               i2=2,
               i3=3,
               i4=4,
               i5=5,
               i7=7,
               )

    s.update_by_object(f)
    d = s.memo.dict
    assert d['lang'] == None
    assert (set(d['terms']) ==
            set([(u'i3:3', 'b', 1), (u'facet:i3', 'b'), (u'2', 'b', 1),
                 (u'i1:1', 'b', 1), (u'i6:42', 'b', 1), (u'i5:5', 'b', 1),
                 (u'i4:4', 'r', 1), (u'i7:7', 'b', 2),
                 ],
                )
            )
    assert set(d['values']) == set([(u'i5', 5, 'integer')])
    assert d['texts'] == []
    assert d['posts'] == []
    name, gots = loads(d['data'])
    gots = [tuple(i) for i in gots]
    assert name == "tests.test_memo.Inter"
    assert set(gots) == set([("i3", "3"), ("i2", "2"), ("i1", "1"),
                             ("i6", "42"), ("i5", "5"), ("i4", "4"),
                             ('i7', '7')])


class Dater(Schema):
    d1 = Date.named('d1')
    d2 = Date.using(prefix=False)
    d3 = Date.using(facet=True)
    d4 = Date.using(boolean=False)
    d5 = Date.using(sortable=True)
    d6 = Date.using(default=datetime.date(2000, 2, 3))
    d7 = Date.using(wdf_inc=2)


def test_date():

    s = Dater.from_defaults()
    f = Object(d1=datetime.date(2001, 1, 1),
               d2=datetime.date(2002, 2, 2),
               d3=datetime.date(2003, 3, 3),
               d4=datetime.date(2004, 4, 4),
               d5=datetime.date(2005, 5, 5),
               d7=datetime.date(2007, 7, 7),
               )

    s.update_by_object(f)
    d = s.memo.dict
    assert d['lang'] == None
    assert (set(d['terms']) ==
            set([(u'd3:20030303', 'b', 1), (u'd5:20050505', 'b', 1),
                 (u'd4:20040404', 'r', 1), (u'd6:20000203', 'b', 1),
                 (u'd1:20010101', 'b', 1), ('20020202', 'b', 1),
                 (u'facet:d3', 'b'), (u'd7:20070707', 'b', 2),
                 ]
                )
            )
    assert set(d['values']) == set([(u'd5', '20050505', 'date')])
    assert d['texts'] == []
    assert d['posts'] == []
    name, gots = loads(d['data'])
    gots = [tuple(i) for i in gots]
    assert name == "tests.test_memo.Dater"
    assert set(gots) == set([(u'd2', u'2002-02-02'), (u'd5', u'2005-05-05'),
                             (u'd1', u'2001-01-01'), (u'd6', u'2000-02-03'),
                             (u'd3', u'2003-03-03'), (u'd4', u'2004-04-04'),
                             (u'd7', u'2007-07-07'),
                             ])


class DateTimer(Schema):
    dt1 = DateTime.named('dt1')
    dt2 = DateTime.using(prefix=False)
    dt3 = DateTime.using(facet=True)
    dt4 = DateTime.using(boolean=False)
    dt5 = DateTime.using(sortable=True)
    dt6 = DateTime.using(default=datetime.datetime(2000, 6, 7, 8, 7, 6))
    dt7 = DateTime.using(wdf_inc=2)


def test_datetime():

    s = DateTimer.from_defaults()
    f = Object(dt1=datetime.datetime(2000, 1, 2, 3, 2, 1),
               dt2=datetime.datetime(2000, 2, 3, 4, 3, 2),
               dt3=datetime.datetime(2000, 3, 4, 5, 4, 3),
               dt4=datetime.datetime(2000, 4, 5, 6, 5, 4),
               dt5=datetime.datetime(2000, 5, 6, 7, 6, 5),
               dt7=datetime.datetime(2000, 7, 8, 9, 7, 6),
               )

    s.update_by_object(f)
    d = s.memo.dict
    assert d['lang'] == None
    assert (set(d['terms']) ==
            set([('20000203', 'b', 1), (u'dt4:20000405', 'r', 1),
                 (u'dt1:20000102', 'b', 1), (u'dt6:20000607', 'b', 1),
                 (u'facet:dt3', 'b'), (u'dt5:20000506', 'b', 1),
                 (u'dt3:20000304', 'b', 1), (u'dt7:20000708', 'b', 2),
                 ],
                )
            )
    assert set(d['values']) == set([(u'dt5', '20000506070605', 'datetime')])
    assert d['texts'] == []
    assert d['posts'] == []
    name, gots = loads(d['data'])
    gots = [tuple(i) for i in gots]
    assert name == "tests.test_memo.DateTimer"
    assert set(gots) == set([(u'dt2', u'2000-02-03 04:03:02'),
                             (u'dt3', u'2000-03-04 05:04:03'),
                             (u'dt4', u'2000-04-05 06:05:04'),
                             (u'dt1', u'2000-01-02 03:02:01'),
                             (u'dt6', u'2000-06-07 08:07:06'),
                             (u'dt5', u'2000-05-06 07:06:05'),
                             (u'dt7', u'2000-07-08 09:07:06'),
])


class Texter(Schema):
    language = String.using(default="en")
    t1 = Text.named('t1')
    t2 = Text.using(prefix=False)
    t3 = Text.using(facet=True)
    t4 = Text.using(stop=False, string=True, string_prefix="foo")
    t5 = Text.using(stem=False, sortable=True)
    t6 = Text.using(default='fourty-two',
                    string=True,
                    string_prefix="bar",
                    boolean=True)
    t7 = Text.using(language='es')
    t8 = Text.using(wdf_inc=2)


def test_text():

    s = Texter.from_defaults()
    f = Object(t1=u'one',
               t2=u'two',
               t3=u'three',
               t4=u'four',
               t5=u'five',
               t7=u'bueno',
               t8=u'many thanks',
               )

    s.update_by_object(f)
    d = s.memo.dict
    assert d['lang'] == u'en'
    assert d['posts'] == []
    assert set(d['terms']) == set([(u'language:en', 'b', 1),
                                   (u'foo:four', 'r', 1),
                                   (u'bar:fourty-two', 'b', 1),
                                   (u'facet:t3', 'b')])

    r = set((i['lang'], i['post'], i['prefix'],
             i['stem'], i['stop'], i['text'], 
             i['wdf_inc'], i['post_start'])
            for i in d['texts'])

    assert r == set(
        [(u'en', True, u't1', True, True, 'one', 1, None),
         (u'en', True, None, True, True, 'two', 1, None),
         (u'en', True, u't3', True, True, 'three', 1, None),
         (u'en', True, u't4', True, False, 'four', 1, None),
         (u'en', True, u't5', False, True, 'five', 1, None),
         (u'en', True, u't6', True, True, 'fourty-two', 1, None),
         ('es', True, u't7', True, True, 'bueno', 1, None),
         ('en', True, u't8', True, True, 'many thanks', 2, None),
         ])

    assert set(d['values']) == set([(u't5', u'five', 'string')])

    name, gots = loads(d['data'])
    gots = [tuple(i) for i in gots]
    assert name == "tests.test_memo.Texter"
    assert set(gots) == set([(u'language', u'en'),
                             (u't1', u'one'),
                             (u't2', u'two'),
                             (u't3', u'three'),
                             (u't4', u'four'),
                             (u't5', u'five'),
                             (u't6', u'fourty-two'),
                             (u't7', u'bueno'),
                             (u't8', u'many thanks'),
                             ])


class Locationer(Schema):
    l1 = Location.named('l1')
    l2 = Location.using(sortable=True)
    l3 = Location.using(boolean=False)
    l4 = Location.using(wdf_inc=2)


def test_location():

    s = Locationer.from_defaults()
    f = Object(l1=(7.0625, -95.677068),
               l2=(8.0625, -25.677068),
               l3=(9.0625, -15.677068),
               l4=(10.0625, -10.677068),
               )

    s.update_by_object(f)
    d = s.memo.dict

    assert d['lang'] == None
    assert set(d['terms']) == set([('loc_watttatcttttgctacgaagt', 'b', 1),
                                   ('loc_wctgtcgccgccctaccgcaag', 'b', 1),
                                   ('loc_wcttgcagtcactgtaaagtga', 'r', 1),
                                   ('loc_wctttaaagtcgtaaaacgatt', 'b', 2),
                                   ])

    assert set(d['values']) == set([(u'l2',
                                     'loc_wctgtcgccgccctaccgcaag',
                                     'location')])
    assert d['texts'] == []
    assert d['posts'] == []
    name, gots = loads(d['data'])
    gots = [(n, tuple(v) if not isinstance(v, basestring) else v) for n, v in gots]
    assert name == "tests.test_memo.Locationer"
    assert set(gots) == set([(u'l3_lon', u'-15.677068'),
                             (u'l3', (9.0625, -15.677068)),
                             (u'l3_lat', u'9.062500'),
                             (u'l2_lat', u'8.062500'),
                             (u'l1', (7.0625, -95.677068000000006)),
                             (u'l2', (8.0625, -25.677067999999998)),
                             (u'l2_lon', u'-25.677068'),
                             (u'l1_lon', u'-95.677068'),
                             (u'l1_lat', u'7.062500'),
                             (u'l4', (10.0625, -10.677068)),
                             (u'l4_lon', u'-10.677068'),
                             (u'l4_lat', u'10.062500'),
                             ])
    

def test_long_term():
    m = Memo()
    assert_raises(InvalidTermError, m.add_term, " " * 250)


class LongOne(Schema):
    ignore_invalid_terms = True
    s1 = String.named('s1')


def test_ignore_invalid():
    s = LongOne.from_defaults()
    f = Object(s1='s1' * 250)
    s.update_by_object(f)
    d = s.memo.dict
    assert not d['terms']
