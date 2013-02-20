"""
Multicorn foreign data wrapper for postgres.

Allows xodb readers to be foreign tables into pg, so xapian can be
queried right from sql (and sqlalchemy!).

ie:

  select id, title from docstore where query='title:bob' order='last_modified';

"""

from multicorn import ForeignDataWrapper

from .database import Database

f = open('dumpquals.txt', 'w')

class XODBFDW(ForeignDataWrapper):

    def __init__(self, options, columns):
        self.options = options
        self.columns = columns
        self.db = Database(
            options['path'],
            spelling=options.get('spelling', False),
            replicated=options.get('replicated', False),
            writable=False)

    def _parse_query_quals(self, quals):
        kwargs = dict(query='')
        for qual in quals:
            name = qual.field_name
            if name == '_x_query':
                kwargs['query'] = qual.value
            if name == '_x_order':
                kwargs['order'] = qual.value
            if name == '_x_offset':
                kwargs['offset'] = qual.value
            if name == '_x_limit':
                kwargs['limit'] = qual.value
            if name == '_x_language':
                kwargs['language'] = qual.value
            if name == '_x_reverse':
                kwargs['reverse'] = qual.value
        return kwargs

    def _parse_estimate_quals(self, quals):
        kwargs = dict(query='')
        for qual in quals:
            name = qual.field_name
            if name == '_x_query':
                kwargs['query'] = qual.value
            if name == '_x_language':
                kwargs['language'] = qual.value
        return kwargs

    def execute(self, quals, columns):
        # f.write(repr(quals))
        # f.flush()
        # for q in quals:
        #     yield dict(name=q.field_name, operator=q.operator, value=q.value)
        kwargs = self._parse_query_quals(quals)
        for r in self.db.query(**kwargs):
            yield {col: kwargs[col[3:]] if col.startswith('_x_') else getattr(r, col) for col in columns}

    # def get_rel_size(self, quals, columns):
    #     args, kwargs = self._parse_estimate_quals(quals)
    #     return (self.db.estimate(*args, **kwargs), self.db.get_avlength())

    # def get_path_keys(self):
    #     pass
