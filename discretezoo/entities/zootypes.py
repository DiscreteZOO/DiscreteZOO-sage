r"""
Type initialization functions

This module provides functions used for type initialization.
"""

import json
import os
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
import discretezoo
from ..db.query import makeFields

# Mappping of strings to types
# Initially contains the builtin Sage types
names = {
    "bool": bool,
    "str": str,
    "Integer": Integer,
    "Rational": Rational,
    "RealNumber": RealNumber
}

# Specification file path
path = os.path.join(discretezoo.__path__[0], "spec")


def normalize_type(t):
    r"""
    Substitute JSON objects with actual classes.

    If ``t`` is a string, returns the corresponding class. If ``t`` is a
    dictionary representing a metaclass, returns a dictionary containing
    the base class and its initalized parameters,

    INPUT:

    - ``t`` -- string or dictionary representing the class.
    """
    if isinstance(t, dict):
        t = dict(t)
        t["class"] = names[t["class"]]
        t["params"] = {str(k): init_fields(v) for k, v in t["params"].items()}
        init_spec(t)
    else:
        t = names[t]
    return t


def init_fields(fields):
    r"""
    Initialize fields in a JSON object.

    Returns a dictionary containing normalized types.

    INPUT:

    - ``fields`` -- a dictionary mapping field names to strings or objects
      representing their types.
    """
    return {str(k): normalize_type(t) for k, t in fields.items()}


def init_fieldparams(fieldparams):
    r"""
    Initialize field parameters in a JSON object.

    Ensures that all parameters are represented as strings.

    INPUT:

    - ``fields`` -- a dictionary mapping field names to strings or lists
      representing the appropriate parameters.
    """
    return {str(k): to_string(p) for k, p in fieldparams.items()}


def init_metaclasses(cl):
    r"""
    Initialize metaclasses belonging to a class.

    Replaces dictionaries representing metaclasses with their parameters
    with actual classes.

    INPUT:

    - ``cl`` -- the class whose metaclasses should be initialized.
    """
    for k, v in cl._spec["fields"].items():
        if isinstance(v, dict):
            t = v["class"](cl, k, v)
            init_metaclasses(t)
            cl._spec["fields"][k] = t


def to_string(s):
    r"""
    Recursively convert elements of a nested list to strings.

    If ``s`` is a string, then it is converted to ``str``. Otherwise, a list
    containg the recursive application of the function to the elements of ``s``
    is returned.

    INPUT:

    - ``s`` -- the object to be converted.
    """
    if isinstance(s, basestring):
        return str(s)
    else:
        return [to_string(x) for x in s]


def register_type(cl):
    r"""
    For a given class, map its name to it.

    INPUT:

    - ``cl`` -- the class to be registered.
    """
    names[cl.__name__] = cl


def init_spec(spec):
    r"""
    Initialize the specification of a class coming from a JSON object.

    Ensures that all keys in the specification are strings,
    and replaces strings with classes where applicable.

    INPUT:

    - ``spec`` - the specification dictionary to be initialized.
    """
    if "indices" in spec:
        spec["indices"] = [tuple(t) if isinstance(t, list) else t
                           for t in to_string(spec["indices"])]
    if "skip" in spec:
        spec["skip"] = to_string(spec["skip"])
    if "noupdate" in spec:
        spec["noupdate"] = to_string(spec["noupdate"])
    if "fieldparams" in spec:
        spec["fieldparams"] = init_fieldparams(spec["fieldparams"])
    if "aliases" in spec:
        spec["aliases"] = {str(k): str(v) for k, v in spec["aliases"].items()}
    if "compute" in spec:
        spec["compute"] = {names[c]: to_string(l)
                           for c, l in spec["compute"].items()}
    if "condition" in spec:
        spec["condition"] = {names[c]: {str(k): v for k, v in d.items()}
                             for c, d in spec["condition"].items()}
    if "default" in spec:
        spec["default"] = {names[c]: {str(k): v for k, v in d.items()}
                           for c, d in spec["default"].items()}


def init_class(cl, fields=None):
    r"""
    Initializes the given class.

    The class specification is read from a JSON file and then initialized.
    If given, a module containing field objects is also initialized.

    INPUT:

    - ``cl`` -- the class to be initialized.

    - ``fields`` (default: ``None``) -- the module to be initialized with
      field objects.
    """
    register_type(cl)
    f = file(os.path.join(path, cl.__name__ + ".json"))
    spec = json.load(f)
    f.close()
    spec["name"] = str(spec["name"])
    spec["primary_key"] = str(spec["primary_key"])
    spec["fields"] = init_fields(spec["fields"])
    init_spec(spec)
    cl._spec = spec
    init_metaclasses(cl)
    if fields is not None:
        makeFields(cl, fields)
