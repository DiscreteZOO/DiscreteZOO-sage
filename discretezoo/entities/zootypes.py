import json
import os
from sage.rings.integer import Integer
from sage.rings.rational import Rational
from sage.rings.real_mpfr import RealNumber
import discretezoo
from ..db.query import makeFields

names = {
    "bool": bool,
    "str": str,
    "Integer": Integer,
    "Rational": Rational,
    "RealNumber": RealNumber
}

path = os.path.join(discretezoo.__path__[0], "spec")

def normalize_type(t):
    if isinstance(t, list):
        t, s, p = t
        t = (names[t], {str(k): init_fields(v) for k, v in s.items()},
             init_fieldparams(p))
    else:
        s = None
        p = None
        t = names[t]
    return t

def init_fields(fields):
    return {str(k): normalize_type(t) for k, t in fields.items()}

def init_fieldparams(fieldparams):
    return {str(k): to_string(p) for k, p in fieldparams.items()}

def init_metaclasses(cl):
    for k, v in cl._spec["fields"].items():
        if isinstance(v, tuple):
            m, f, p = v
            t = m(cl, k, fieldparams = p, **f)
            init_metaclasses(t)
            cl._spec["fields"][k] = t

def to_string(s):
    if isinstance(s, basestring):
        return str(s)
    else:
        return [to_string(x) for x in s]

def register_type(cl):
    names[cl.__name__] = cl

def init_class(cl, fields = None):
    register_type(cl)
    f = file(os.path.join(path, cl.__name__ + ".json"))
    spec = json.load(f)
    f.close()
    spec["name"] = str(spec["name"])
    spec["primary_key"] = str(spec["primary_key"])
    spec["indices"] = [tuple(t) if isinstance(t, list) else t
                       for t in to_string(spec["indices"])]
    spec["skip"] = to_string(spec["skip"])
    spec["fields"] = init_fields(spec["fields"])
    spec["fieldparams"] = init_fieldparams(spec["fieldparams"])
    spec["compute"] = {names[c]: to_string(l)
                        for c, l in spec["compute"].items()}
    spec["default"] = {names[c]: {str(k): v for k, v in d.items()}
                        for c, d in spec["default"].items()}
    cl._spec = spec
    init_metaclasses(cl)
    if fields is not None:
        makeFields(cl, fields)
