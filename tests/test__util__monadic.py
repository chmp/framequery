from __future__ import print_function, division, absolute_import

from framequery.util import _monadic as m

from framequery.parser import ast as a


def test__record():
    assert _apply(m.wildcard, a.Integer('42')) == ([a.Integer('42')], [])
    assert _apply(m.record(a.Integer, value=m.wildcard), a.Integer('42')) == (['42'], [])

    assert _apply(m.eq('42'), '42') == (['42'], [])
    assert _apply(m.eq('42'), '43') == (None, ['43'])

    matcher = m.any(
        m.record(a.Integer, value=m.eq('42')),
        m.record(a.Name, name=m.eq('foo')),
    )

    assert _apply(matcher, a.Name('foo')) == (['foo'], [])
    assert _apply(matcher, a.Integer('42')) == (['42'], [])


def test__unpack():
    assert m.match('42', m.capture(m.eq('42'))) == m.MatchResult(True, {0: ['42']})

    matcher = m.record(a.Integer, value=m.capture(m.eq('42')))
    assert m.match(a.Integer('42'), matcher) == m.MatchResult(True, {0: ['42']})

    matcher = m.capture(m.instanceof(a.Integer), group=0)
    assert m.match(a.Integer('42'), matcher) == m.MatchResult(True, {0: [a.Integer('42')]})

    # NOTE the , to iterate over the result
    v, = m.match(a.Integer('42'), matcher)
    assert v == a.Integer('42')


def _apply(m, v):
    m, r, _ = m([v])
    return m, r[:1]
