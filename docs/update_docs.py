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

        is_up_todate = (
            os.path.exists(target) and
            os.path.getmtime(target) > os.path.getmtime(__file__) and
            os.path.getmtime(target) > os.path.getmtime(source)
        )

        if is_up_todate:
            _logger.info('%s is up to date', target)
            continue

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
        if line.startswith('.. autofunction::'):
            lines += autofunction(line)

        elif line.startswith('.. autoclass::'):
            lines += autoclass(line)

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

def autoclass(line):
    # TODO: document members
    return autoobject(line)


def automodule(line):
    # TODO: document members
    return autoobject(line)


def autoobject(line):
    _, what = line.split('::')
    obj = import_object(what)

    if inspect.isfunction(obj):
        signature = format_signature(what, obj)

    elif inspect.isclass(obj):
        signature = format_signature(what, obj.__init__, skip=1)

    else:
        signature = ''

    yield '## {}'.format(what)

    if signature:
        yield signature

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

    return '`{}({})`'.format(label, ', '.join(args))


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
        yield '[{0}](#{0})'.format(node.attributes['title'], node.attributes['reference'])


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


def import_object(what):
    mod, _, what = what.rpartition('.')
    mod = mod.strip()
    what = what.strip()

    mod = importlib.import_module(mod)
    return getattr(mod, what)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
