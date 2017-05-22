"""Support a small subset of sphinx features in plain markdown files.
"""
from __future__ import print_function, division, absolute_import

import importlib
import inspect
import logging
import os
import os.path

from docutils.core import publish_string
from docutils.nodes import Element
from docutils.parsers.rst import roles
from docutils.writers import Writer

_logger = logging.getLogger(__name__)


def main():
    setup_rst_roles()

    self_path = os.path.dirname(__file__)
    docs_dir = os.path.abspath(os.path.join(self_path, '..'))
    src_dir = os.path.abspath(os.path.join(self_path, '..', 'docs', 'src'))

    for fname in relwalk(src_dir):
        if not fname.endswith('.md'):
            continue

        source = os.path.abspath(os.path.join(src_dir, fname))
        target = os.path.abspath(os.path.join(docs_dir, fname))

        # NOTE: always generate docs, to include newest docstrings

        _logger.info('transform %s -> %s', source, target)

        with open(source, 'rt') as fobj:
            content = fobj.read()

        content = transform(content, source)

        with open(target, 'wt') as fobj:
            fobj.write(content)


def setup_rst_roles():
    roles.register_canonical_role('class', rewrite_reference)
    roles.register_canonical_role('func', rewrite_reference)


def rewrite_reference(name, rawtext, text, lineno, inliner, options=None, content=None):
    # TODO: support titles
    return [TitledReference(rawtext, reference=text, title=text)], []


class TitledReference(Element):
    pass


def relwalk(absroot, relroot='.'):
    for fname in os.listdir(absroot):
        relpath = os.path.join(relroot, fname)
        abspath = os.path.join(absroot, fname)

        if fname in {'.', '..'}:
            continue

        if os.path.isfile(abspath):
            yield relpath

        elif os.path.isdir(abspath):
            yield from relwalk(abspath, relpath)


def transform(content, source):
    lines = []
    for line in content.splitlines():
        if line.startswith('.. include::'):
            lines += include(line, source)

        elif line.startswith('.. autofunction::'):
            lines += autofunction(line)

        elif line.startswith('.. autoclass::'):
            lines += autoclass(line)

        elif line.startswith('.. automethod::'):
            lines += automethod(line)

        elif line.startswith('.. automodule::'):
            lines += automodule(line)

        elif line.startswith('.. literalinclude::'):
            lines += literalinclude(line, source)

        elif line.startswith('..'):
            raise NotImplementedError('unknown directive: %s' % line)

        else:
            lines.append(line)

    return '\n'.join(lines)


def autofunction(line):
    return autoobject(line)


def automethod(line):
    return autoobject(line, depth=2, skip_args=1)


def autoclass(line):
    return autoobject(line)


def automodule(line):
    return autoobject(line, depth=0)


def autoobject(line, depth=1, skip_args=0):
    _, what = line.split('::')

    if '(' in what:
        signature = what
        what, _1, _2 = what.partition('(')

    else:
        signature = None

    obj = import_object(what, depth=depth)

    if signature is None:
        if inspect.isfunction(obj):
            signature = format_signature(what, obj, skip=skip_args)

        elif inspect.isclass(obj):
            signature = format_signature(what, obj.__init__, skip=1 + skip_args)

        else:
            signature = ''

    yield '## {}'.format(what)

    if signature:
        yield '`{}`'.format(signature)

    yield ''
    yield render_docstring(obj)


def format_signature(label, func, skip=0):
    args = inspect.getfullargspec(func)
    args, varargs, keywords, defaults = args[:4]

    args = args[skip:]
    if not defaults:
        defaults = []

    varargs = [] if varargs is None else [varargs]
    keywords = [] if keywords is None else [keywords]

    args = (
        ['{}'.format(arg) for arg in args[:len(defaults)]] +
        ['{}={!r}'.format(arg, default) for arg, default in zip(args[-len(defaults):], defaults)] +
        ['*{}'.format(arg) for arg in varargs] +
        ['**{}'.format(arg) for arg in keywords]
    )

    return '{}({})'.format(label.strip(), ', '.join(args))


def literalinclude(line, source):
    _, what = line.split('::')
    what = what.strip()

    what = os.path.abspath(os.path.join(os.path.dirname(source), what))
    _, ext = os.path.splitext(what)

    type_map = {
        '.py': 'python',
        '.sh': 'bash',
    }

    with open(what, 'r') as fobj:
        content = fobj.read()

    yield '```' + type_map.get(ext.lower(), '')
    yield content
    yield '```'


def include(line, source):
    _, what = line.split('::')
    what = what.strip()

    what = os.path.abspath(os.path.join(os.path.dirname(source), what))

    with open(what, 'r') as fobj:
        content = fobj.read()

    yield content


def render_docstring(obj):
    doc = obj.__doc__ or '<undocumented>'
    doc = unindent(doc)

    return publish_string(
        doc,
        writer=MarkdownWriter(),
        settings_overrides={'output_encoding': 'unicode'}
    )


class MarkdownWriter(Writer):
    def translate(self):
        self.output = ''.join(self._translate(self.document))

    def _translate(self, node):
        func = '_translate_{}'.format(type(node).__name__)
        try:
            func = getattr(self, func)

        except AttributeError:
            raise NotImplementedError('cannot translate %r (%r)' % (node, node.astext()))

        return func(node)

    def _translate_children(self, node):
        for c in node.children:
            yield from self._translate(c)

    _translate_document = _translate_children

    def _translate_paragraph(self, node):
        yield from self._translate_children(node)
        yield '\n\n'

    def _translate_literal_block(self, node):
        yield '```\n'
        yield node.astext()
        yield '\n'
        yield '```\n'
        yield '\n'

    def _translate_Text(self, node):
        yield node.astext()

    def _translate_literal(self, node):
        yield '`{}`'.format(node.astext())

    def _translate_field_name(self, node):
        # TODO: parse parameter definitions, i.e., param str name -> param name (str).
        yield '**{}** '.format(node.astext())

    _translate_field_list = _translate_field = _translate_field_body = _translate_children

    def _translate_TitledReference(self, node):
        yield '[{0}](#{1})'.format(
            node.attributes['title'],
            node.attributes['reference'].replace('.', '').lower(),
        )


def unindent(doc):
    def impl():
        lines = doc.splitlines()
        indent = find_indent(lines)

        if lines:
            yield lines[0]

        for line in lines[1:]:
            yield line[indent:]

    return '\n'.join(impl())


def find_indent(lines):
    for line in lines[1:]:
        if not line.strip():
            continue

        return len(line) - len(line.lstrip())

    return 0


def import_object(what, depth=1):
    parts = what.split('.')

    if depth > 0:
        mod = '.'.join(parts[:-depth]).strip()
        what = parts[-depth:]

    else:
        mod = '.'.join(parts).strip()
        what = []

    obj = importlib.import_module(mod)

    for p in what:
        obj = getattr(obj, p)

    return obj


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
