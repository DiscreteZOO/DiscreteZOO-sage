from sage.rings.integer import Integer

def lookup(d, k):
    if k in d and d[k] is not None:
        return d[k]
    raise KeyError(k)

def update(d, k, v):
    d[k] = v
    # TODO: write to the database and prepare a commit

def todict(r, skip = []):
    return {k: Integer(r[k]) if isinstance(r[k], int) else r[k] for k in r.keys() if k not in skip}

def isinteger(x):
    return isinstance(x, Integer) or isinstance(x, int)