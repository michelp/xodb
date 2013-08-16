from collections import OrderedDict

# LRUDict is cargoed from Idealist.org


class lazy_property(object):
    """Decorator, a @property that is only evaluated once per instance."""

    def __init__(self, fn):
        self.fn = fn
        self.__name__ = fn.func_name
        self.__doc__ = fn.__doc__

    def __get__(self, obj, cls):
        if obj is None:
            return None
        obj.__dict__[self.__name__] = result = self.fn(obj)
        return result


class LRUDict(OrderedDict):
    """
    A simple data structure to provide item ordering based on usage for a
    limited dictionary size. The last item in the dictionary will always be
    considered the most recently used. By the same logic, the item at the front
    will be the least recently used. If the dictionary is filled beyond the
    limit, items at the front of the list will be discarded.
    """

    def __init__(self, items=(), limit=1000, on_cache_miss=None):
        self.limit = limit
        super(LRUDict, self).__init__(items)

    def __setitem__(self, key, value):
        if len(self) >= self.limit:
            self.popitem()
        super(LRUDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        item = dict.pop(self, key)
        self[key] = item
        return item

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
