from framequery.query import contains_aggregates


def test_contains_aggregates():
    assert contains_aggregates('SELECT 2 + 3 FROM DUAL') == False
    assert contains_aggregates('SELECT SUM(a) FROM b') == True
