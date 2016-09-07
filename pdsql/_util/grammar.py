from funcparserlib.parser import maybe, pure, some


def optional(parser):
    return maybe(parser) | pure(None)


def token(ttype, value=None):
    return some(lambda t:
        t.ttype is ttype and (value is None or t.value == value)
    )


def failing():
    return some(lambda t: False)
