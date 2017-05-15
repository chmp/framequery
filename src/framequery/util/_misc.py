from __future__ import print_function, division, absolute_import

import collections
import itertools as it


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
    return bool(unpack(obj, matcher))


def unpack(obj, matcher):
    """Select variables based on a matcher.

    Example::

        res = unpack(obj, a.Name(Any(0))

        if res:
            name = res.get(0)

    """
    if matcher is Any:
        return UnpackResult(True)

    elif isinstance(matcher, Matcher):
        return matcher.unpack(obj)

    else:
        return UnpackResult(obj == matcher)


def unpack_all(obj, matcher):
    for item in obj:
        m = unpack(item, matcher)
        if m:
            yield m


class UnpackResult(object):
    @classmethod
    def make(cls, matched, group, val):
        if not matched:
            return cls(False)

        if group is None:
            return cls(True)

        return cls(True, {group: [val]})

    def __init__(self, matched=False, matches=None):
        if matches is None:
            matches = {}

        self.matched = bool(matched)
        self.matches = dict(matches)

    def get(self, idx):
        result, = self.getall(idx)
        return result

    def getall(self, idx):
        return self.matches.get(idx, [])

    def __bool__(self):
        return self.matched

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.matched is other.matched and
            self.matches == other.matches
        )

    def __repr__(self):
        return 'UnpackResult(%s, %s)' % (self.matched, self.matches)

    def __or__(self, other):
        if self.matched is False or other.matched is False:
            return UnpackResult(False)

        return UnpackResult(self.matched, {
            k: self.matches.get(k, []) + other.matches.get(k, [])
            for k in set(self.matches) | set(other.matches)
        })

    def __iter__(self):
        if not self.matched:
            raise ValueError()
        return iter(self.get(idx) for idx in sorted(self.matches))

    def __getitem__(self, item):
        return self.get(item)


class Matcher(object):
    def unpack(self, obj):
        raise NotImplementedError()


class Any(Matcher):
    def __init__(self, group):
        self.group= group

    def unpack(self, obj):
        return UnpackResult.make(True, self.group, obj)


class OneOf(Matcher):
    def __init__(self, *matchers):
        self.matchers = matchers

    def unpack(self, obj):
        for m in self.matchers:
            m = unpack(obj, m)
            if m:
                return m

        return UnpackResult(False)


class In(Matcher):
    def __init__(self, *values, **kwargs):
        self.values = set(values)
        self.group = kwargs.get('group')

    def unpack(self, obj):
        return UnpackResult.make(obj in self.values, self.group, obj)


class Not(Matcher):
    def __init__(self, matcher, group=None):
        self.matcher = matcher
        self.group = group

    def unpack(self, obj):
        if unpack(obj, self.matcher):
            return UnpackResult(False)

        return UnpackResult.make(True, self.group, obj)


class Eq(Matcher):
    def __init__(self, value, group=None):
        self.value = value
        self.group = group

    def unpack(self, obj):
        return UnpackResult.make(obj == self.value, self.group, obj)


class Literal(Matcher):
    def __init__(self, value, group=None):
        self.value = value
        self.group = group

    def unpack(self, _):
        return UnpackResult.make(True, self.group, self.value)


class Capture(Matcher):
    def __init__(self, matcher, group=None):
        self.matcher = matcher
        self.group = group

    def unpack(self, obj):
        return UnpackResult.make(bool(unpack(obj, self.matcher)), self.group, obj)


class Transform(Matcher):
    def __init__(self, func, matcher):
        self.func = func
        self.matcher = matcher

    def unpack(self, obj):
        m = unpack(obj, self.matcher)
        return UnpackResult(m.matched, {
            k: [self.func(v) for v in vv] for k, vv in m.matches.items()
        })


class ExactSequence(Matcher):
    def __init__(self, *matchers):
        self.matchers = matchers

    def unpack(self, obj):
        if not isinstance(obj, collections.Sequence):
            return UnpackResult(False)

        if len(obj) != len(self.matchers):
            return UnpackResult(False)

        result = UnpackResult(True)
        for o, m in zip(obj, self.matchers):
            result |= unpack(o, m)

        return result


class Sequence(Matcher):
    def __init__(self, matcher):
        self.matcher = matcher

    def unpack(self, obj):
        if not isinstance(obj, (list, tuple)):
            return UnpackResult(False)

        result = UnpackResult(True)
        for item in obj:
            result |= unpack(item, self.matcher)

        return result


class InstanceOf(Matcher):
    def __init__(self, cls, group=None):
        self.cls = cls
        self.group = None

    def unpack(self, obj):
        return UnpackResult.make(isinstance(obj, self.cls), self.group, obj)


class Record(Matcher):
    """Class to simplify creating typed nodes in ASTs, etc.."""
    __fields__ = ()
    __types__ = ()

    def __init__(self, *args, **kwargs):
        unknown  = set(kwargs) - set(self.__fields__)
        if unknown:
            raise ValueError('unknown fields: {}'.format(unknown))

        kwargs.update(dict(zip(self.__fields__, args)))

        types = dict(zip(self.__fields__, self.__types__))

        for key in self.__fields__:
            type = types.get(key, lambda x: x)

            val = kwargs.get(key)
            if val is not None and val is not Any and not isinstance(val, Matcher):
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

    def update(self, **kwargs):
        values = self.key()
        values = dict(zip(self.__fields__, values))
        values.update(kwargs)
        return self.__class__(**values)

    @classmethod
    def any(cls, **kwargs):
        items = {k: Any for k in cls.__fields__}
        items.update(**kwargs)
        return cls(**items)

    def unpack(self, obj):
        if type(self) is not type(obj):
            return UnpackResult(False)

        result = UnpackResult(True)
        for obj_k, self_k in zip(obj.key(), self.key()):
            result |= unpack(obj_k, self_k)

        return result


class RuleSet(object):
    @classmethod
    def make(cls, name=None, rules=()):
        def impl(root):
            return cls(rules=rules, name=name, root=root)

        return impl

    def __init__(self, rules=(), name=None, root=None):
        if root is not None:
            self.root = root

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
        return self.root(self, obj, *args)

    def root(self, _, obj, *args):
        return self.apply_rules(obj, *args)

    def apply_rules(self, obj, *args):
        for m, t in self.rules:
            if match(obj, m):
                return t(self, obj, *args)

        raise ValueError('not support, no rule matches {}'.format(obj))

    def __repr__(self):
        if self.name is not None:
            return 'RuleSet(name={}, ...)'.format(self.name)

        return 'RuleSet(...)'
