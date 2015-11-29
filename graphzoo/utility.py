from sage.rings.integer import Integer
from sage.rings.real_mpfr import create_RealNumber
from inspect import getargspec
from query import Column

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

def default(d, k, v = None):
    if k not in d:
        d[k] = v

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
        return Integer(rows[0][0])
    elif len(dims) == 1:
        return {r[1]: Integer(r[0]) for r in rows}
    d = {}
    dims = [k.alias if isinstance(k, Column) else str(k) for k in dims]
    for r in rows:
        dd = d
        for i in range(len(dims)):
            v = r[dims[i]]
            if i == len(dims)-1:
                dd[v] = Integer(r[0])
            else:
                if v not in dd:
                    dd[v] = (dims[i+1], {})
                dd = dd[v][1]
    return (dims[0], d)

def todict(r, db):
    return {k: db.from_db_type(v, type(v))
            for k, v in dict(r).items() if v is not None}

def construct(cl, self, d):
    argspec = getargspec(cl.__init__)
    if argspec[2] is None:
        d = {k: v for k, v in d.items() if k in argspec[0]}
    return cl.__init__(self, **d)
