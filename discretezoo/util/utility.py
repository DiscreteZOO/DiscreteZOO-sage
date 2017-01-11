r"""
Utility functions

This module contains utility functions used throughout the package.
"""

from sage.rings.integer import Integer
from sage.rings.real_mpfr import create_RealNumber
from inspect import getargspec
from ..db.query import Column

def lookup(d, k, destroy = False, **kargs):
    r"""
    Lookup a key in a dictionary.

    Returns the value at key ``k`` if it exists and it is not ``None``,
    otherwise raise ``KeyError``.

    INPUT:

    - ``d`` -- the dictionary to look up in.

    - ``k`` -- the key to look up for.

    - ``destroy`` (default ``False``) -- whether to delete ``d[k]`` if it
      exists.

    - ``default`` - if specified, the value to return instead of raising an
      error (must be a named parameter).
    """
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
    r"""
    Set a default value if none exists.

    If the dictionary ``d`` does not have the key ``k``, it is set with value
    ``v``.

    INPUT:

    - ``d`` -- the dictionary to use.

    - ``k`` -- the key to use.

    - ``v`` (default ``None``) -- the value to set.
    """
    if k not in d:
        d[k] = v

def update(d, k, v):
    r"""
    Set a value in the dictionary.

    INPUT:

    - ``d`` -- the dictionary to use.

    - ``k`` -- the key to use.

    - ``v`` -- the value to set.
    """
    d[k] = v

def isinteger(x):
    r"""
    Decide whether ``x`` is an integer.

    Returns ``True`` is ``x`` is either a Sage ``Integer`` or a Python ``int``.
    """
    return isinstance(x, Integer) or isinstance(x, int)

def int_or_real(x):
    r"""
    Convert ``x`` to a Sage integer or real number.

    If ``x`` is integral, an ``Integer`` is returned. Otherwise, a real number
    is returned.
    """
    if isinteger(x):
        return Integer(x)
    else:
        return create_RealNumber(x)

def tomultidict(rows, dims):
    r"""
    Output a multidimensional counting structure with labeled dimensions.

    If ``dims`` is zero, a single integer is returned. If ``dims`` is one,
    a simple dictionary is returned. Otherwise, returns a pair containing the
    name of the dimension and a dictionary whose keys are values taken at the
    dimension, and whose values are integers in the last dimensions, or the
    same structure at other dimensions.

    INPUT:

    - ``rows`` -- an iterable containing rows with data, with the counts
      at column 0 of each row.

    - ``dims`` -- a list of columns representing the dimensions.
    """
    if len(dims) == 0:
        return Integer(rows[0][0])
    elif len(dims) == 1:
        return {r[1]: Integer(r[0]) for r in rows}
    d = {}
    dims = [k.colalias if isinstance(k, Column) else str(k) for k in dims]
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
    r"""
    Construct a dictionary from a row, skipping ``None``s.

    Row elements are converted to Sage objects in accordance to the rules set
    by the database.

    INPUT:

    - ``r`` -- the row containing the data.

    - ``db`` -- the database being used.
    """
    return {k: db.from_db_type(v, type(v))
            for k, v in dict(r).items() if v is not None}

def construct(cl, self, d):
    r"""
    Construct an object of a given class.

    INPUT:

    - ``cl`` -- the class to be used.

    - ``self`` -- the object to be initalized.

    - ``d`` -- the dictionary containing the parameters.
    """
    argspec = getargspec(cl.__init__)
    if argspec.keywords is None:
        d = {k: v for k, v in d.items() if k in argspec.args}
    cl.__init__(self, **d)
