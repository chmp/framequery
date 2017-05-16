from __future__ import print_function, division, absolute_import

import os

import pytest
import sqlalchemy

from framequery.util._misc import Record, record_diff


@pytest.fixture
def database():
    if 'FQ_TEST_DB' in os.environ:
        return sqlalchemy.create_engine(os.environ['FQ_TEST_DB'])

    pytest.skip('test db not available')


def pytest_assertrepr_compare(op, left, right):
    if not(isinstance(left, Record) and isinstance(right, Record) and op == "=="):
        return

    return [
        'Comparing Records',
        '%r != %r' % (left, right),
    ] + list(record_diff(left, right))

