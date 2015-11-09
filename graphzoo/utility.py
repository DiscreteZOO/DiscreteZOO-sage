from sage.rings.integer import Integer
from sage.rings.real_mpfr import create_RealNumber

def lookup(d, k, destroy = False, **kargs):
    if k in d:
        v = d[k]
        if destroy:
            del d[k]
        if v is not None:
            return v
    if "default" in kargs:
        return kargs["default"]
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

def tomultidict(rows, dims):
    if len(dims) == 0:
        return rows[0][0]
    elif len(dims) == 1:
        return {r[1]: r[0] for r in rows}
    d = {}
    for r in rows:
        dd = d
        for i in range(len(dims)):
            v = r[dims[i]]
            if i == len(dims)-1:
                dd[v] = r[0]
            else:
                if v not in dd:
                    dd[v] = (dims[i+1], {})
                dd = dd[v][1]
    return (dims[0], d)

def drop_none(r):
    return {k: v for k, v in dict(r).items() if v is not None}
