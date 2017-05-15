"""Helpers to write monadic parsers
"""
from __future__ import print_function, division, absolute_import

import enum
import itertools as it
import logging
import re

_logger = logging.getLogger(__name__)


class Status(enum.Enum):
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
        kwargs.setdefault('message', None)
        return dict(status=s, **kwargs)


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


def new_interface(matcher):
    """Adapt a matcher to the old interface"""
    def new_interface_impl(seq):
        m, s, _ = matcher(seq)
        return m, s

    return new_interface_impl


def fail(msg):
    def impl(seq):
        raise ValueError(msg)
    return impl


def list_of(sep, item):
    return transform(
        lambda obj: [obj],
        flat_sequence(item, many(flat_sequence(sep, item)))
    )


def debug(msg, matcher):
    def impl(seq):
        m, s = matcher(seq)

        if m is not None:
            print('{} match {}'.format(msg, seq[:10]))

        else:
            print('{} not match {}'.format(msg, seq[:10]))

        return m, s

    return impl


class define(object):
    """Definition of recursive parsers"""
    def __init__(self, factory=None):
        self.factory = factory
        self._parser = None

    def __call__(self, seq):
        if self._parser is None:
            self._parser = self.factory(self)

        return self._parser(seq)


def literal(val):
    return lambda seq: ([val], seq, Status.succeed())


def optional(matcher):
    def optional_impl(seq):
        m, s, d = matcher(seq)

        if m is None:
            return [], seq, Status.succeed(children=[d], where='optional')

        return m, s, Status.succeed(children=[d], where='optional')

    return optional_impl


def flat_sequence(*matchers):
    return transform(lambda seq: list(it.chain.from_iterable(seq)), sequence(*matchers))


def sequence(*matchers):
    def sequence_impl(seq):
        result = []
        results = []
        s = seq

        for matcher in matchers:
            m, s, d = _call(matcher, s)

            results += [d]
            if m is None:
                return None, seq, Status.fail(children=results, where='sequence')

            result += [m]

        return result, s, Status.succeed(children=results, where='sequence')

    return sequence_impl


def many(matcher):
    def many_impl(seq):
        parts = []
        children = []

        while True:
            p, seq, d = matcher(seq)

            if p is None:
                break

            parts += p
            children.append(d)

        return parts, seq, Status.succeed(consumed=0, children=children, where='many')

    return many_impl


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


def token(matcher):
    def token_impl(seq):
        if not seq:
            return None, seq, Status.fail(mesage='no input', where='token')

        m, r, d = matcher(seq[0])

        if m is None:
            return None, seq, Status.fail(children=[d])

        elif r:
            return None, seq, Status.fail(message='remaining: {}'.format(r), children=[d], where='token')

        return m, seq[1:], Status.succeed(children=[d], where='token')

    return token_impl


def consume(n):
    def consume_impl(seq):
        if len(seq) < n:
            return None, seq, Status.fail(children=[], message='not enough inputs', where='consume')

        return seq[:n], seq[n:], Status.succeed()

    return consume_impl


def skip(matcher):
    @_delegate(matcher, where='skip')
    def skip_impl(m, r, d, _):
        return [], r, d

    return skip_impl


class construct(object):
    def __init__(self, cls, matcher, *matchers):
        self.cls = cls

        if matchers:
            self.matcher = flat_sequence(matcher, *matchers)

        else:
            self.matcher = matcher

    def __call__(self, seq):
        matches, rest, debug  = self.matcher(seq)
        if matches is None:
            return None, seq, Status.fail(children=[debug], where='construct')

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

        return [res], rest, Status.succeed(children=[debug], where='construct')


def keyword(**kw):
    (name, matcher), = kw.items()
    return map(lambda obj: {name: obj}, matcher)


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


def regex(p):
    p = re.compile(p)

    def impl(s):
        m = p.match(s)

        if m is None:
            return None, s, Status.fail(consumed=0)

        r = m.group(0)
        return [r], s[len(r):], Status.succeed(consumed=len(r))

    return impl


def string(quote='\'', escape='\\', find=str.find):
    def impl(s):
        if not s:
            return None, s, Status.fail(consumed=0)

        if s[0] != quote:
            return None, s, Status.fail(consumed=0)

        idx = 0
        while idx >= 0:
            idx = find(s, quote, idx + 1)

            if idx < 0:
                return None, s, Status.fail(consumed=idx+1)

            if s[idx - 1] != escape:
                break

        return [s[:idx + 1]], s[idx + 1:], Status.succeed(consumed=idx + 1)

    return impl
