from __future__ import print_function, division, absolute_import

from .parser import as_parsed
from ._visitor import Visitor


def contains_aggregates(ast_or_string):
    ast = as_parsed(ast_or_string)

    v = ContainsAggregatesVisitor()
    v.visit(ast)
    return v.result


class ContainsAggregatesVisitor(Visitor):
    def __init__(self):
        self.result = False

    def enter_general_set_function(self, node):
        self.result = True
