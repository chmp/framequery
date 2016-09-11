from __future__ import print_function, division, absolute_import

from funcparserlib.parser import forward_decl, many, skip

from .tokenize import tokenize


class Node(object):
    @classmethod
    def _parser(cls):
        return cls.parser

    @classmethod
    def parse(cls, q):
        stmt = tokenize(q)
        return cls.get_parser().parse(stmt)

    @classmethod
    def get_parser(cls):
        return cls._parser() >> cls.from_parsed

    @classmethod
    def from_parsed(cls, val):
        return cls(val)

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        try:
            other_key = other.key

        except AttributeError:
            return NotImplemented

        return self.key() == other_key()

    def __repr__(self):
        key = self.key()
        head, tail = key[0], key[1:]

        return '{}({})'.format(head.__name__, ', '.join(repr(v) for v in tail))

    def key(self):
        return self.__class__, self.value


class ForwardDecl(Node):
    @classmethod
    def _parser(cls):
        try:
            return cls._lazy_parser

        except AttributeError:
            cls._lazy_parser = forward_decl()
            return cls._lazy_parser

    @classmethod
    def define(cls, p):
        if callable(p):
            return p(cls)

        cls._parser().define(p >> cls.from_parsed)

    @classmethod
    def get_parser(cls):
        return cls._parser()


class Unvalued(Node):
    @classmethod
    def from_parsed(cls, val):
        return cls()

    def __init__(self):
        pass

    def key(self):
        return self.__class__,


class TransparentNode(Node):
    """After parsing a transparent node is replace with its value.
    """
    @classmethod
    def from_parsed(cls, val):
        return val


class ListNode(Node):
    """Its value should be a tupe of the form ``(head, tail)``.
    """
    @classmethod
    def from_parsed(cls, val):
        head, tail = val
        return [head] + list(tail)

    @classmethod
    def _parser(cls):
        p = (
            cls.item_parser +
            many(skip(cls.separator_parser) + cls.item_parser)
        )

        prefix = getattr(cls, 'prefix_parser', None)
        if prefix is not None:
            p = skip(prefix) + p

        return p


class Record(Node):
    def __init__(self, *args, **kwargs):
        kwargs.update(dict(zip(self.__fields__, args)))

        for key in self.__fields__:
            setattr(self, key, kwargs.get(key))

    def __repr__(self):
        cls, items = self.key()
        kv_pairs = ', '.join('{}={!r}'.format(k, v) for (k, v) in items.items())
        return '{}({})'.format(self.__class__.__name__, kv_pairs)

    def key(self):
        self_dict = {k: getattr(self, k) for k in self.__fields__}
        return self.__class__, self_dict

    def with_values(self, **kwargs):
        _, values = self.key()
        values.update(kwargs)
        return self.__class__(**values)


class RecordNode(Record, Node):
    @classmethod
    def from_parsed(cls, val):
        return cls(**dict(val))
