import sqlparse
from sqlparse.tokens import Token


__all__ = ['tokenize', 'Tokens']


# TODO: normalize tokens, to become more independent of sqlparse
def tokenize(s):
    # TODO: figure out how to handle newlines proper
    s = s.replace('\n', ' ')

    stmt = single(sqlparse.parse(s))
    return list(
        token for token in stmt.flatten()
        if token.ttype is not Tokens.Whitespace
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
    Float = Token.Literal.Number.Float
    String = Token.Literal.String.Single

    Name = Token.Name
    Operator = Token.Operator
    Comparison = Token.Operator.Comparison

    Keyword = Token.Keyword
    DML = Token.Keyword.DML
    Order = Token.Keyword.Order
    CTE = Token.Keyword.CTE

    Wildcard = Token.Wildcard


def main():
    import sys

    for q in sys.argv[1:]:
        tokenized = tokenize(q)
        print([(p, p.ttype) for p in tokenized])


if __name__ == "__main__":
    main()
