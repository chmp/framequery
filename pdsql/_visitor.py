from __future__ import print_function, division, absolute_import

import six

import collections

from ._base import RecordNode
from ._util.introspect import node_name_to_handler_name


class Visitor(object):
    def visit(self, node):
        self._call_handler('enter', node)

        for child in self._call_handler('children', node):
            self.visit(child)

        self._call_handler('exit', node)

    def enter_not_found(self, node):
        pass

    def exit_not_found(self, node):
        pass

    def children_not_found(self, node):
        if isinstance(node, RecordNode):
            _, values = node.key()
            return list(
                item for item in values.values()
                if item is not None
            )

        if isinstance(node, list):
            return node

        print("cannot recurse", self._get_class_name(node))
        return []

    def children(self, node):
        impl = self._get_handler(node, prefix='children')
        return impl(node)

    def _call_handler(self, prefix, node):
        handler = self._get_handler(prefix, node)
        return handler(node)

    def _get_handler(self, prefix, node):
        class_name = self._get_class_name(node)
        handler_name = node_name_to_handler_name(class_name, prefix)

        default = getattr(self, '{}_not_found'.format(prefix))
        return getattr(self, handler_name, default)

    @staticmethod
    def _get_class_name(node):
        return node.__class__.__name__
