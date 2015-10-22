from sage.rings.integer import Integer

def lookup(d, k):
    if k in d and d[k] is not None:
        return d[k]
    raise KeyError(k)

def todict(r, skip = []):
    return {k: Integer(r[k]) if isinstance(r[k], int) else r[k] for k in r.keys() if k not in skip}
