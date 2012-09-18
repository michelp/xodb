# OrderedDict is cargoed from SQLAlchemy,
# LRUDict is cargoed from Idealist.org


class OrderedDict(dict):
    """A dict that returns keys/values/items in the order they were added."""

    def __init__(self, ____sequence=None, **kwargs):
        self._list = []
        if ____sequence is None:
            if kwargs:
                self.update(**kwargs)
        else:
            self.update(____sequence, **kwargs)

    def clear(self):
        self._list = []
        dict.clear(self)

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return OrderedDict(self)

    def sort(self, *arg, **kw):
        self._list.sort(*arg, **kw)

    def update(self, ____sequence=None, **kwargs):
        if ____sequence is not None:
            if hasattr(____sequence, 'keys'):
                for key in ____sequence.keys():
                    self.__setitem__(key, ____sequence[key])
            else:
                for key, value in ____sequence:
                    self[key] = value
        if kwargs:
            self.update(kwargs)

    def setdefault(self, key, value):
        if key not in self:
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)

    def __iter__(self):
        return iter(self._list)

    def values(self):
        return [self[key] for key in self._list]

    def itervalues(self):
        return iter(self.values())

    def keys(self):
        return list(self._list)

    def iterkeys(self):
        return iter(self.keys())

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def iteritems(self):
        return iter(self.items())

    def __setitem__(self, key, object):
        if key not in self:
            try:
                self._list.append(key)
            except AttributeError:
                # work around Python pickle loads() with 
                # dict subclass (seems to ignore __setstate__?)
                self._list = [key]
        dict.__setitem__(self, key, object)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._list.remove(key)

    def pop(self, key, *default):
        present = key in self
        value = dict.pop(self, key, *default)
        if present:
            self._list.remove(key)
        return value

    def popitem(self):
        item = dict.popitem(self)
        self._list.remove(item[0])
        return item


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
