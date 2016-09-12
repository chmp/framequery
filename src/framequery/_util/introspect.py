from __future__ import print_function, division, absolute_import

import re


def call_handler(self, prefix, node, *args, **kwargs):
    node_type = node.__class__.__name__
    handler_name = node_name_to_handler_name(node_type, prefix=prefix)

    handler = getattr(self, handler_name, None)
    if handler is None:
        raise ValueError('cannot handle {} (no handler {})'.format(node, handler_name))

    return handler(node, *args, **kwargs)


def node_name_to_handler_name(name, prefix='visit'):
    return '{}_{}'.format(prefix, _camelcase_to_python(name))


def _camelcase_to_python(s):
    # taken from http://stackoverflow.com/a/1176023
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
    s = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()
    return s
