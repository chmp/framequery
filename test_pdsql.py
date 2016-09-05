from pdsql import Integer, Name, Operator, Select


def test_integer_simple():
    assert Integer.parse('2').eval({}) == 2
    assert Integer.parse('42').eval({}) == 42


def test_name_simple():
    assert Name.parse('a').eval({'a': 42}) == 42


def test_operator():
    import operator
    assert Operator.parse('+').eval({}) is operator.add


def test_select_integer():
    assert Select.parse('SELECT 42').eval({}) == [42]


def test_select_addition():
    assert Select.parse('SELECT 3 + 5').eval({}) == [8]


def test_select_addition():
    assert Select.parse('SELECT 3 + 5, 2+2').eval({}) == [8, 4]


def test_select_name():
    scope = {
        'tbl': {'a': 42}
    }
    assert Select.parse('SELECT a FROM tbl').eval(scope) == [42]
    assert Select.parse('SELECT a + 13 FROM tbl').eval(scope) == [55]
