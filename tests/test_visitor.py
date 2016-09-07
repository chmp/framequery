from pdsql._visitor import *


def test_example():
    assert node_name_to_handler_name('Select') == 'visit_select'
    assert node_name_to_handler_name('HavingClause') == 'visit_having_clause'


def test_example_handle():
    assert node_name_to_handler_name('Select', 'handle') == 'handle_select'
    assert node_name_to_handler_name('HavingClause', 'handle') == 'handle_having_clause'
