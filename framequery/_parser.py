from .parser import *


def get_selected_column_name(node):
    if isinstance(node, ColumnReference):
        return node.value[-1]

    if not isinstance(node, DerivedColumn):
        return None

    if node.alias is not None:
        return node.alias

    return get_selected_column_name(node.value)
