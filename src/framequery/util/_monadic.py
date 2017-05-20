"""Helpers to write monadic parsers

This mechanism is used in three ways inside framequery:

- parse sequences into sequences (tokenize)
- parse sequences into object trees (parsing)
- extract parts of an object tree (matching)

"""
from __future__ import print_function, division, absolute_import

import logging
import re

_logger = logging.getLogger(__name__)


def match(val, matcher):
    m, r, d = matcher([val])

    if m is None:
        return MatchResult(False)

    if r:
        raise ValueError('not fully consumed {}: {}'.format(r, '\n'.join(format_debug(d))))

    matches = {}

    for v in m:
        if isinstance(v, CaptureGroup):
            matches.setdefault(v.key, []).append(v.val)

    return MatchResult(True, matches)


class MatchResult(object):
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

    # backwards compatibility for py27
    __nonzero__ = __bool__

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
            return MatchResult(False)

        return MatchResult(self.matched, {
            k: self.matches.get(k, []) + other.matches.get(k, [])
            for k in set(self.matches) | set(other.matches)
        })

    def __iter__(self):
        if not self.matched:
            raise ValueError()
        return iter(self.get(idx) for idx in sorted(self.matches))

    def __getitem__(self, item):
        return self.get(item)


class RuleSet(object):
    """A dispatcher based on matching rules"""
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
            self.add(m, t)

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


def format_debug(debug, indent=0):
    status = debug.get('status', '/')
    message = debug.get('message', '')
    children = debug.get('children', [])
    where = debug.get('where', '<unknown>')

    yield('{}{}: {} in {}'.format(' ' * indent, status, message, where))

    for d in children:
        for msg in format_debug(d, indent=indent + 1):
            yield msg


class Status(object):
    success = 'success'
    failure = 'failure'

    @classmethod
    def succeed(cls, **kwargs):
        return cls._gen(cls.success, **kwargs)

    @classmethod
    def fail(cls, **kwargs):
        return cls._gen(cls.failure, **kwargs)

    @classmethod
    def _gen(cls, s, **kwargs):
        kwargs.setdefault('children', [])
        kwargs.setdefault('message', '')
        return dict(status=s, **kwargs)


def capture(matcher, group=0):
    @_delegate(matcher, where='capture')
    def capture_impl(m, r, d, seq):
        return [CaptureGroup(group, v) for v in m], r, d

    return capture_impl


class CaptureGroup(object):
    def __init__(self, key, val):
        self.key = key
        self.val = val


def _delegate(matcher, func=None, **kwargs):
    def delegate_impl(func):
        def delegated(seq):
            m, r, d = _call(matcher, seq)

            if m is None:
                # TODO: use r here?
                return None, seq, Status.fail(children=[d], **kwargs)

            return func(m, r, Status.succeed(children=[d], **kwargs), seq)

        return delegated

    return delegate_impl if func is None else delegate_impl(func)


def _call(matcher, seq):
    try:
        m, r, d = matcher(seq)

        if not isinstance(d, dict):
            raise ValueError('invalid debug message')

        return m, r, d

    except Exception as exc:
        # TODO: use exception chaining?
        raise RuntimeError('parsing error in %s: %s' % (matcher, exc))


class define(object):
    """Definition of recursive parsers"""
    def __init__(self, factory=None):
        self.factory = factory
        self._parser = None

    def __call__(self, seq):
        if self._parser is None:
            self._parser = self.factory(self)

        return self._parser(seq)


def literal(*vals):
    """Insert values into the result list"""
    return lambda seq: (list(vals), seq, Status.succeed(where='insert'))


def optional(matcher):
    def optional_impl(seq):
        m, s, d = matcher(seq)

        if m is None:
            return [], seq, Status.succeed(children=[d], where='optional')

        return m, s, Status.succeed(children=[d], where='optional')

    return optional_impl


def sequence(*matchers):
    def sequence_impl(seq):
        result = []
        children = []
        s = seq

        for matcher in matchers:
            m, s, d = _call(matcher, s)

            children.append(d)
            if m is None:
                return None, seq, Status.fail(children=children, where='sequence')

            result += m

        return result, s, Status.succeed(children=children, where='sequence')

    return sequence_impl


def repeat(matcher):
    def repeat_impl(seq):
        parts = []
        children = []

        while True:
            p, seq, d = matcher(seq)

            children.append(d)
            if p is None:
                break

            parts += p

        return parts, seq, Status.succeed(children=children, where='repeat')

    return repeat_impl


def any(*matchers):
    def any_impl(s):
        children = []
        for m in matchers:
            m, r, d = _call(m, s)

            children.append(d)
            if m is not None:
                return m, r, Status.succeed(children=children, where='any')

        return None, s, Status.fail(consumed=0, children=children, where='any')

    return any_impl


def one(matcher):
    def one_impl(seq):
        if not seq:
            return None, seq, Status.fail(where='one', message='no items')

        m, r, d = matcher(seq[0])

        if m is None:
            return None, seq, Status.fail(where='one', children=[d], message='did not match')

        if r:
            return None, seq, Status.fail(where='one', children=[d], message='not fully consumed')

        return m, seq[1:], Status.succeed(where='one', children=[d])

    return one_impl


def lit(val):
    """Accept a single item and return the given value."""
    return lambda seq: ([val], seq[1:], Status.succeed(where='lit'))


def rep(matcher):
    """Accept a single list and match each item against the matcher."""
    return one(repeat(matcher))


def verb(*p):
    """Match a single item against the given prototypes verbatim."""
    # NOTE: do not rely on verbatim, since verbatim is relying on the sequence interface
    return any(*[eq(v) for v in p])


def ignore(matcher):
    @_delegate(matcher, where='ignore')
    def skip_impl(m, r, d, _):
        return [], r, d

    return skip_impl


def map_capture(f, matcher):
    return transform(
        lambda seq: [
            CaptureGroup(item.key, f(item.val))
            for item in seq
            if isinstance(item, CaptureGroup)
        ],
        matcher,
    )


def map(f, matcher):
    return transform(lambda seq: [f(item) for item in seq], matcher)


def transform(f, matcher):
    @_delegate(matcher, where='transform')
    def transform_impl(m, r, d, _):
        return f(m), r, d

    return transform_impl


def verbatim(*p):
    return map_verbatim(lambda x: x, *p)


def map_verbatim(f, *p):
    by_length = {}
    for c in p:
        by_length.setdefault(len(c), set()).add(c)

    # tend to longest tokens first in case they overlap with shorter ones
    n_max = max(by_length)
    by_length = sorted(by_length.items(), reverse=True)

    def impl(s):
        for n, c in by_length:
            r = f(s[:n])
            if r in c:
                return [r], s[n:], Status.succeed(
                    message='%r matched' % r,
                    where='map_verbatim'
                )

        return None, s, Status.fail(
            message='%r does not match any in %r' % (s[:n_max], set(p)),
            where='map_verbatim',
        )

    return impl


# ************************************************
# **            higher level parsers            **
# ************************************************
def list_of(sep, item):
    return transform(
        lambda obj: [obj],
        sequence(item, repeat(sequence(sep, item)))
    )


def regex(p):
    p = re.compile(p)

    def impl(s):
        m = p.match(s)

        if m is None:
            return None, s, Status.fail(consumed=0)

        r = m.group(0)
        return [r], s[len(r):], Status.succeed(consumed=len(r))

    return impl


# NOTE: do not use str.find to avoid py2 incompatibility
def string(quote='\'', escape='\\', find=lambda s, n, i: s.find(n, i)):
    def impl(s):
        if not s:
            return None, s, Status.fail(consumed=0)

        if s[0] != quote:
            return None, s, Status.fail(consumed=0)

        idx = 0
        while idx >= 0:
            idx = find(s, quote, idx + 1)

            if idx < 0:
                return None, s, Status.fail(consumed=idx + 1)

            if s[idx - 1] != escape:
                break

        return [s[:idx + 1]], s[idx + 1:], Status.succeed(consumed=idx + 1)

    return impl


def pred(func):
    def pred_impl(obj):
        if not obj:
            return [], obj, Status.fail(where='pred<%s>' % func, message='no input')

        if not func(obj[0]):
            return None, obj, Status.fail(
                where='pred<%s>' % func,
                message='predicate %s failed' % func,
            )

        return [obj[0]], obj[1:], Status.succeed(where='pred<%s>' % func)

    return pred_impl


def eq(val):
    return pred(lambda obj: obj == val)


def ne(val):
    return pred(lambda obj: obj != val)


def instanceof(cls):
    return pred(lambda obj: isinstance(obj, cls))


wildcard = pred(lambda _: True)


# *************************************************
# **   object support: build and match objects   **
# *************************************************
class construct(object):
    def __init__(self, cls, matcher, *matchers):
        self.cls = cls

        if matchers:
            self.matcher = sequence(matcher, *matchers)

        else:
            self.matcher = matcher

    def __call__(self, seq):
        matches, rest, debug = self.matcher(seq)
        if matches is None:
            return None, seq, Status.fail(children=[debug], where='construct %s' % self.cls)

        kw = {}
        for d in matches:
            duplicates = set(kw) & set(d)
            if duplicates:
                raise ValueError('duplicate keys: {}'.format(duplicates))

            kw.update(d)

        try:
            res = self.cls(**kw)

        except Exception as exc:
            raise RuntimeError('error constructing %s: %s' % (self.cls, exc))

        return [res], rest, Status.succeed(children=[debug], where='construct %s' % self.cls)


def keyword(**kw):
    (name, matcher), = kw.items()
    return map(lambda obj: {name: obj}, matcher)


def record(*args, **kwargs):
    cls, args = args[0], args[1:]

    if args:
        kwargs.update(dict(zip(cls.__fields__, args)))

    def record_impl(obj):
        if not obj:
            return None, obj, {}

        if type(obj[0]) is not cls:
            return None, obj, {
                'where': 'record', 'children': [], 'message': 'different type of {!r}'.format(obj),
            }

        result = []
        debug = {'where': 'record', 'children': [], 'keys': []}

        for k, mm in sorted(kwargs.items()):
            v = getattr(obj[0], k)
            m, r, d = mm([v])

            debug['children'].append(d)
            debug['keys'].append(k)

            if m is None:
                return None, obj, debug

            elif r:
                return None, obj, debug

            else:
                result.extend(m)

        return result, obj[1:], debug

    return record_impl
