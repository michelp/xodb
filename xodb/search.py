from operator import attrgetter
from itertools import imap
from xapian import Query


class Search(object):
    """Generative query building interface.

    Nice for generating more complex query objects but avoiding the
    string formatting/parsing issues.
    """

    def __init__(self, db, query, 
                 language=None, limit=None,
                 order=None, reverse=False):
        if isinstance(query, basestring):
            query = db.querify(query)
        self.query = query

        self._db = db
        self._language = language
        self._limit = limit
        self._order = order
        self._reverse = reverse

    def copy(self, query):
        return type(self)(self._db, query,
                          language=self._language,
                          limit=self._limit,
                          order=self._order,
                          reverse=self._reverse)

    def operator(self, query, op):
        """Wrap self with an operator and another query.
        """
        query = self._db.querify(query, language=self._language)
        return self.copy(Query(op, self.query, query))

    def filter(self, query):
        return self.operator(query, Query.OP_FILTER)

    def and_(self, query):
        return self.operator(query, Query.OP_AND)

    def or_(self, query):
        return self.operator(query, Query.OP_OR)

    def and_not(self, query):
        return self.operator(query, Query.OP_AND_NOT)

    def xor(self, query):
        return self.operator(query, Query.OP_XOR)

    def and_maybe(self, query):
        return self.operator(query, Query.OP_MAYBE)

    def near(self, query):
        return self.operator(query, Query.OP_NEAR)

    def expand(self, limit=10, mlimit=100):
        candidates = self.suggest(limit, mlimit)
        return self.or_(candidates)

    def limit(self, limit):
        return type(self)(self._db, self.query,
                          language=self._language,
                          order=self._order,
                          reverse=self._reverse,
                          limit=limit)
        
    def language(self, language):
        return type(self)(self._db, self.query,
                          language=language)

    def order(self, order):
        return type(self)(self._db, self.query,
                          order=order)

    def reverse(self, reverse):
        return type(self)(self._db, self.query, reverse=reverse)

    def count(self):
        return self._db.count(self.query, language=self._language)

    def estimate(self):
        return self._db.estimate(self.query, language=self._language)

    def suggest(self, limit=10, mlimit=100):
        return list(self._db.suggest(
            self.query, language=self._language,
            limit=limit, mlimit=mlimit))

    @property
    def records(self):
        """Generator over xapian results.
        """
        return self._db.query(
            self.query, limit=self._limit, language=self._language,
            order=self._order, reverse=self._reverse)

    @property
    def uids(self):
        """Generator for matching uids.
        """
        return imap(attrgetter('uid'), self.records)
