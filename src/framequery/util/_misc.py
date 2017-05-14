from __future__ import print_function, division, absolute_import

import itertools as it


class Record(object):
    """Class to simplify creating typed nodes in ASTs, etc.."""
    __fields__ = ()

    def __init__(self, *args, **kwargs):
        unknown  = set(kwargs) - set(self.__fields__)
        if unknown:
            raise ValueError('unknown fields: {}'.format(unknown))

        kwargs.update(dict(zip(self.__fields__, args)))

        for key in self.__fields__:
            setattr(self, key, kwargs.get(key))

    def __eq__(self, other):
        try:
            other_key = other.key

        except AttributeError:
            return NotImplemented

        return self.key() == other_key()

    def __repr__(self):
        kv_pairs = ', '.join('{}={!r}'.format(k, getattr(self, k)) for k in self.__fields__)
        return '{}({})'.format(self.__class__.__name__, kv_pairs)

    def key(self):
        self_dict = {k: getattr(self, k) for k in self.__fields__}
        return self.__class__, self_dict

    def update(self, **kwargs):
        _, values = self.key()
        values.update(kwargs)
        return self.__class__(**values)

    @classmethod
    def any(cls, **kwargs):
        items = {k: Any for k in cls.__fields__}
        items.update(**kwargs)
        return cls(**items)


def record_diff(a, b):
    if type(a) is not type(b):
        return ['error: %r != %r' % (a, b)]

    if isinstance(a, list):
        if len(a) != len(b):
           return ['length differ: %r != %r' % (a, b)]

        return it.chain.from_iterable(record_diff(u, v) for (u, v) in zip(a, b))

    elif not isinstance(a, Record):
        return [] if a == b else ['error %r != %r' % (a, b)]

    ac, ak = a.key()
    bc, bk = b.key()

    keys = set(ak) | set(bk)

    return it.chain.from_iterable(record_diff(ak[k], bk[k]) for k in keys)



def match(obj, matcher):
    if matcher is Any:
        return True

    elif isinstance(matcher, Matcher):
        return matcher(obj)

    if type(obj) is not type(matcher):
        return False

    if isinstance(matcher, Record):
        return all(match(a, b) for a, b in zip(obj.key(), matcher.key()) )

    elif isinstance(matcher, list):
        return len(obj) == len(matcher) and all(match(a, b) for a, b in zip(obj, matcher))

    elif isinstance(matcher, dict):
        return set(matcher) == set(obj) and all(match(obj[k], matcher[k]) for k in obj.keys())

    else:
        return obj == matcher


class Any(object):
    pass


class Matcher(object):
    def __call__(self, obj):
        raise NotImplementedError()


class In(Matcher):
    def __init__(self, *values):
        self.values = set(values)

    def __call__(self, obj):
        return obj in self.values


class Sequence(Matcher):
    def __init__(self, matcher):
        self.matcher = matcher

    def __call__(self, obj):
        if not isinstance(obj, (list, tuple)):
            return False

        return all(match(item, self.matcher) for item in obj)


class InstanceOf(Matcher):
    def __init__(self, cls):
        self.cls = cls

    def __call__(self, obj):
        return isinstance(obj, self.cls)


class RuleSet(object):
    def __init__(self, rules=(), name=None):
        self.name = name
        self.rules = []

        for m, t in rules:
            self.add(m ,t)

    def rule(self, matcher):
        def rule_impl(transform):
            self.add(matcher, transform)
            return transform
        return rule_impl

    def add(self, matcher, transform):
        # TODO: precompute dispatcher table based on matcher class
        self.rules.append((matcher, transform))

    def __call__(self, obj, *args):

        for m, t in self.rules:
            if match(obj, m):
                return t(self, obj, *args)

        raise ValueError('not support, no rule matches {}'.format(obj))

    def __repr__(self):
        if self.name is not None:
            return 'RuleSet(name={}, ...)'.format(self.name)

        return 'RuleSet(...)'
