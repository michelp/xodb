"""
Multicorn foreign data wrapper for postgres.

Allows xodb readers to be foreign tables into pg, so xapian can be
queried right from sql (and sqlalchemy!).

ie:

  select id, title from docstore where query='title:bob' order='last_modified';

"""

from multicorn import ForeignDataWrapper


class XODBFDW(ForeignDataWrapper):

    def __init__(self, options. columns):
        self.options = options
        self.columns = columns
        self.db = Database(
            options['path'],
            spelling=options.get('spelling', True),
            replicated=options.get('replicated', False),
            writable=False)

    def _parse_query_quals(self, quals):
        args = []
        kwargs = {}
        for qual in quals:
            name = qual.field_name
            if name == 'query':
                args.append(qual.value)
            if name == 'order':
                kwargs['order'] = options.value
            if name == 'offset':
                kwargs['offset'] = options.value
            if name == 'limit':
                kwargs['limit'] = options.value
            if name == 'language':
                kwargs['language'] = options.value
            if name == 'reverse':
                kwargs['reverse'] = options.value
        return args, kwargs

    def _parse_estimate_quals(self, quals):
        args = []
        kwargs = {}
        for qual in quals:
            name = qual.field_name
            if name == 'query':
                args.append(qual.value)
            if name == 'language':
                kwargs['language'] = options.value
        return args, kwargs

    def execute(self, quals, columns):
        args, kwargs = self._parse_query_quals(quals)
        for r in self.db.query(*args, **kwargs):
            yield {col: getattr(r, col) for col in columns}

    def get_rel_size(self, quals, columns):
        args, kwargs = self._parse_estimate_quals(quals)
        return (self.db.estimate(*args, **kwargs), self.db.get_avlength())

    # def get_path_keys(self):
    #     pass
