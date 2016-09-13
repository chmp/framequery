from __future__ import print_function, division, absolute_import

from ._util.introspect import call_handler

import itertools as it
import subprocess


def to_dot(node):
    return GraphvizGenerator().compile(node)


def show(node):
    source = to_dot(node)

    from IPython.display import Image
    p = subprocess.Popen(
        ['dot', '-Tpng'], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    stdout, _ = p.communicate(source.encode('utf8'))
    return Image(data=stdout)


class GraphvizGenerator(object):
    def __init__(self):
        self.id_generator = id_generator()

    def compile(self, root):
        source = [
            u'digraph dag {'
        ]

        root_id = self._new_label()
        source.extend(self.compile_node(root, root_id))

        source.extend([
            u'}'
        ])

        return u'\n'.join(source)

    def compile_node(self, node, label):
        return call_handler(self, 'compile_node', node, label)

    def compile_node_drop_duplicates(self, node, label):
        child_source, child_id = self._compile_child(node.table)

        return child_source + [
            u'{} -> {}'.format(label, child_id),
            u'{} [label="DropDuplicates"]'.format(label),
        ]

    def compile_node_transform(self, node, label):
        child_source, child_id = self._compile_child(node.table)

        return child_source + [
            u'{} -> {}'.format(label, child_id),
            u'{} [label="Transform"]'.format(label),
        ]

    def compile_node_aggregate(self, node, label):
        child_source, child_id = self._compile_child(node.table)

        return child_source + [
            u'{} -> {}'.format(label, child_id),
            u'{} [label="Aggregate"]'.format(label),
        ]

    def compile_node_join(self, node, label):
        left_source, left_id = self._compile_child(node.left)
        right_source, right_id = self._compile_child(node.right)

        return left_source + right_source + [
            u'{} -> {} [label="left"]'.format(label, left_id),
            u'{} -> {} [label="right"]'.format(label, right_id),
            u'{} [label="Join"]'.format(label),
        ]

    def compile_node_sort(self, node, label):
        child_source, child_id = self._compile_child(node.table)

        return child_source + [
            u'{} -> {}'.format(label, child_id),
            u'{} [label="Sort"]'.format(label),
        ]

    def compile_node_limit(self, node, label):
        child_source, child_id = self._compile_child(node.table)

        return child_source + [
            u'{} -> {}'.format(label, child_id),
            u'{} [label="Limit"]'.format(label),
        ]

    def compile_node_filter(self, node, label):
        child_source, child_id = self._compile_child(node.table)
        return child_source  +[
            u'{} -> {}'.format(label, child_id),
            u'{} [label="Filter"]'.format(label),
        ]

    def compile_node_get_table(self, node, label):
        return [
            u'{} [label="GetTable"]'.format(label),
        ]

    def _compile_child(self, node):
        child_id = self._new_label()
        child_source = self.compile_node(node, child_id)

        return child_source, child_id

    def _new_label(self):
        return next(self.id_generator)


def id_generator():
    for i in it.count():
        yield 'node{}'.format(i)
