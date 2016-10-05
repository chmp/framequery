from __future__ import print_function, division, absolute_import

import itertools as it


def default_id_generator():
    for i in it.count():
        yield '${}'.format(i)
