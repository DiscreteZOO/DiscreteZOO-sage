from sage.rings.integer import Integer
from sage.rings.real_mpfr import create_RealNumber

def lookup(d, k, default = None):
    if k in d and d[k] is not None:
        return d[k]
    if default is not None:
        return default
    raise KeyError(k)

def update(d, k, v):
    d[k] = v
    # TODO: write to the database and prepare a commit

def isinteger(x):
    return isinstance(x, Integer) or isinstance(x, int)

def int_or_real(x):
    if isinteger(x):
        return Integer(x)
    else:
        return create_RealNumber(x)
