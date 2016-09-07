from __future__ import print_function, division, absolute_import

import operator

from funcparserlib.parser import some, maybe, skip, finished, forward_decl, many, pure

from .tokenize import tokenize, Tokens


pure_whitespace = some(lambda t: t.ttype is Tokens.Whitespace)
whitespace = skip(pure_whitespace)
optional_whitespace = skip(maybe(pure_whitespace))

comma = skip(some(lambda t: t.ttype is Tokens.Punctuation and t.value == ','))

pure_semicolon = some(lambda t: t.ttype is Tokens.Punctuation and t.value == ';')
optional_semicolon = skip(maybe(pure_semicolon))


class Node(object):
    @classmethod
    def parse(cls, q):
        stmt = tokenize(q)
        print(stmt)
        return cls.get_parser().parse(stmt)

    @classmethod
    def get_parser(cls):
        return cls.parser >> cls.from_parsed

    @classmethod
    def from_parsed(cls, val):
        return cls(val)


class ForwardDecl(Node):
    @classmethod
    def _parser(cls):
        try:
            return cls._lazy_parser

        except AttributeError:
            cls._lazy_parser = forward_decl()
            return cls._lazy_parser

    @classmethod
    def define(cls, p):
        cls._parser().define(p >> cls.from_parsed)

    @classmethod
    def get_parser(cls):
        return cls._parser()


class Value(Node):
    @classmethod
    def from_parsed(cls, holder):
        return cls(holder.value)

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        cls = self.__class__.__name__
        return '{}({!r})'.format(cls, self.value)

    def eval(self, table):
        return self.eval_value(self.value)


class Literal(Node):
    @classmethod
    def get_parser(cls):
        return Integer.get_parser()


class Integer(Value):
    parser = some(lambda t: t.ttype is Tokens.Integer)
    eval_value = int


class Name(Value):
    parser = some(lambda t: t.ttype is Tokens.Name)

    def eval(self, table):
        return table[self.value]


class Operator(Node):
    parser = some(lambda t: t.ttype is Tokens.Operator)

    def __init__(self, operator):
        self.operator = get_operator(operator)

    def eval(self, scope):
        return self.operator


class Term(ForwardDecl):
    @classmethod
    def from_parsed(cls, p):
        head, tail = p

        for op, right in tail:
            head = BinaryExpression(op, head, right)

        return head


class BinaryExpression(object):
    def __init__(self, operator, left, right):
        self.operator = operator
        self.left = left
        self.right = right

    def eval(self, scope):
        operator = self.operator.eval(scope)
        left = self.left.eval(scope)
        right = self.right.eval(scope)

        return operator(left, right)


Term.define(
    (Literal.get_parser() | Name.get_parser()) +
    many(
        optional_whitespace +
        Operator.get_parser() +
        optional_whitespace +
        Term.get_parser()
    )
)


class TermList(list, ForwardDecl):
    @classmethod
    def from_parsed(cls, p):
        first, rest = p

        res = cls([first])
        res.extend(item for l in rest for item in l)
        return res


TermList.define(
    Term.get_parser() +
    many(
        optional_whitespace +
        comma +
        optional_whitespace +
        TermList.get_parser()
    )
)

class Select(Node):
    select = some(lambda t: t.ttype is Tokens.DML and t.value == 'SELECT')
    from_ = some(lambda t: t.ttype is Tokens.Keyword and t.value == 'FROM')

    selectable = (
        (whitespace + skip(from_) + whitespace + Name.get_parser()) |
        pure(None)
    )

    parser = (
        skip(select) +
        skip(whitespace) +
        TermList.get_parser() +
        selectable +
        optional_semicolon +
        skip(finished)
    )
    
    @classmethod
    def from_parsed(cls, parts):
        terms, selectable = parts

        return cls(
            columns=terms,
            table=Table(selectable.value if selectable is not None else 'DUAL'),
        )

    def __init__(self, columns, table):
        self.columns = columns
        self.table = table

    def eval(self, scope):
        table = self.table.eval(scope)
        return [col.eval(table) for col in self.columns]


class Table(object):
    def eval(self, scope):
        if self.name == 'DUAL':
            return Dual()

        else:
            return scope[self.name]

    def __init__(self, name):
        self.name = name


class Dual(object):
    def __getitem__(self, key):
        raise KeyError("cannot get from dual")


def get_operator(token):
    if token.value == '*':
        return operator.mul

    if token.value == '+':
        return operator.add

    raise ValueError()
