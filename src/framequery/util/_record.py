from __future__ import print_function, division, absolute_import

import collections
import itertools as it


class Record(object):
    """Class to simplify creating typed nodes in ASTs, etc.."""
    __fields__ = ()
    __types__ = ()

    def __init__(self, *args, **kwargs):
        unknown = set(kwargs) - set(self.__fields__)
        if unknown:
            raise ValueError('unknown fields: {}'.format(unknown))

        kwargs.update(dict(zip(self.__fields__, args)))

        types = dict(zip(self.__fields__, self.__types__))

        for key in self.__fields__:
            type = types.get(key)

            val = kwargs.get(key)
            if val is not None and type is not None:
                val = type(val)

            setattr(self, key, val)

    def __eq__(self, other):
        try:
            other_key = other.key

        except AttributeError:
            return NotImplemented

        return self.key() == other_key()

    def __hash__(self):
        return hash((type(self),) + self.key())

    def __repr__(self):
        kv_pairs = ', '.join('{}={!r}'.format(k, getattr(self, k)) for k in self.__fields__)
        return '{}({})'.format(self.__class__.__name__, kv_pairs)

    def key(self):
        return tuple(getattr(self, k) for k in self.__fields__)

    def items(self):
        return zip(self.__fields__, self.key())

    def update(self, **kwargs):
        values = self.key()
        values = dict(zip(self.__fields__, values))
        values.update(kwargs)
        return self.__class__(**values)


def diff(a, b):
    if type(a) is not type(b):
        return ['error: %r != %r' % (a, b)]

    if isinstance(a, list):
        if len(a) != len(b):
            return ['length differ: %r != %r' % (a, b)]

        return it.chain.from_iterable(diff(u, v) for (u, v) in zip(a, b))

    elif not isinstance(a, Record):
        return [] if a == b else ['error %r != %r' % (a, b)]

    ak = dict(a.items())
    bk = dict(b.items())

    keys = set(ak) | set(bk)

    return it.chain.from_iterable(diff(ak[k], bk[k]) for k in keys)


def walk(obj):
    if isinstance(obj, collections.Mapping):
        yield obj
        for value in obj.values():
            for item in walk(value):
                yield item

    elif isinstance(obj, collections.Sequence) and not isinstance(obj, str):
        yield obj
        for value in obj:
            for item in walk(value):
                yield item

    elif isinstance(obj, Record):
        yield obj
        for value in obj.key():
            for item in walk(value):
                yield item

    else:
        yield obj
