import sqlparse
from sqlparse.tokens import Token


__all__ = ['tokenize', 'Tokens']


def tokenize(s):
    stmt = single(sqlparse.parse(s))
    return list(
        token for token in stmt.flatten()
        if not token.ttype is Tokens.Whitespace
    )


def single(s):
    result = list(zip((1, 2), s))

    if len(result) != 1:
        raise ValueError()

    return result[0][1]


class Tokens(object):
    Whitespace = Token.Text.Whitespace
    Punctuation = Token.Punctuation

    Integer = Token.Literal.Number.Integer
    Name = Token.Name
    Operator = Token.Operator

    Keyword = Token.Keyword
    DML = Token.Keyword.DML

    Wildcard = Token.Wildcard

def main():
    import sys

    for q in sys.argv[1:]:
        print(tokenize(q))


if __name__ == "__main__":
    main()
