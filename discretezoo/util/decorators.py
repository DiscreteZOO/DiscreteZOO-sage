r"""
Method decorators

This module provides some decorators for class methods.
"""

import re
import discretezoo
from inspect import getargspec
from .utility import isinteger
from .utility import lookup
from .utility import update
from ..db.query import Expression
from ..entities.zooobject import ZooObject


def parse(obj, exp):
    r"""
    Evaluate an expression with values from ``obj``.

    INPUT:

    - ``obj`` - the object to get the properties from

    - ``exp`` - the expression to evaluate
    """
    if isinstance(exp, basestring):
        return lookup(obj._getprops(exp), exp)
    elif isinstance(exp, Expression):
        return exp.eval(lambda e: parse(obj, e))
    else:
        raise TypeError


class ZooDecorator(object):
    r"""
    A class providing decorators for overriding methods of a given class.
    """
    cl = None

    def __init__(this, cl):
        r"""
        Object constructor.

        INPUT:

        - ``cl`` - the class whose methods will be overridden.
        """
        this.cl = cl

    def documented(this, fun, attr=None):
        r"""
        Embed the overridden function's documentation.

        If the overriding function has a docstring of its own, it is inserted
        after the INPUT section of the overridden functions's docstring.
        Otherwise, the inserted documentation describes the parameters
        ``store`` and ``cur``.

        INPUT:

        - ``attr`` - if specified, the docstring of ``attr`` will be used
          instead of the docstring of the overridden function.
        """
        doc = []
        basedoc = None
        end = None
        try:
            if attr is None:
                attr = type.__getattribute__(this.cl, fun.func_name)
            basedoc = attr.__doc__
        except AttributeError:
            pass
        if basedoc is not None:
            m = re.search(r'^\s*(OUTPUT|ALGORITHM|EXAMPLES?|TESTS?):', basedoc,
                          re.MULTILINE)
            if m is None:
                doc.append(basedoc)
            else:
                st = m.start()
                doc.append(basedoc[:st])
                end = basedoc[st:]
        if fun.__doc__ is None:
            s = ''
            if basedoc is not None:
                m = re.search(r'^ +', basedoc, re.MULTILINE)
                if m is not None:
                    s = m.group()
            doc.append(r"""
{0}DiscreteZOO-specific parameters:

{0}- ``store`` - whether to store the computed results back to the database
{0}  (must be a named parameter; default: ``discretezoo.WRITE_TO_DB``).

{0}- ``cur`` - the cursor to use for database interaction (must be a named
{0}  parameter; default: ``None``).
{0}""".format(s))
        else:
            doc.append(fun.__doc__)
        if end is not None:
            doc.append(end)
        fun.__doc__ = "\n".join(doc)
        return fun

    def computed(this, acceptArgs=None):
        r"""
        Wrap the computing function with database interaction.

        The decorated function should compute the desired property. It should
        accept the following parameters:

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).

        INPUT:

        - ``acceptArgs`` - if specified, the decorated function should
          return a pair containing the value to be stored in the database
          and the actual output. The decorated function will be called only
          if all of the specified arguments are present in ``acceptArgs``.
        """
        def _computed(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store", default=discretezoo.WRITE_TO_DB,
                               destroy=True)
                cur = lookup(kargs, "cur", default=None, destroy=True)
                default = len(largs) + len(kargs) == 0
                cl = self._getclass(fun.func_name)
                d = self._getprops(cl)
                try:
                    if not default:
                        raise NotImplementedError
                    a = lookup(d, fun.func_name)
                    if issubclass(cl._spec["fields"][fun.func_name],
                                  ZooObject) and isinteger(a):
                        a = cl._spec["fields"][fun.func_name](zooid=a)
                        update(d, fun.func_name, a)
                    return a
                except (KeyError, NotImplementedError):
                    a = fun(self, store=store, cur=cur, *largs, **kargs)
                    if acceptArgs is None:
                        out = a
                    else:
                        a, out = a
                        args = getargspec(fun).args
                        default = all(arg in acceptArgs
                                      for arg in args[1:len(largs)+1]) and \
                            all(arg in acceptArgs for arg in kargs)
                    if default:
                        if store:
                            if isinstance(a, ZooObject):
                                v = a._zooid
                            else:
                                v = a
                            self._update_rows(cl, {fun.func_name: v},
                                              {self._spec["primary_key"]:
                                               self._zooid}, cur=cur)
                        update(d, fun.func_name, a)
                    return out
            decorated.func_name = fun.func_name
            return this.documented(decorated, fun)
        return _computed

    def derived(this, fun):
        r"""
        Derive a property from other properties.

        The decorated function should derive the desired property. It should
        accept the following parameters:

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).
        """
        def decorated(self, *largs, **kargs):
            store = lookup(kargs, "store", default=discretezoo.WRITE_TO_DB,
                           destroy=True)
            cur = lookup(kargs, "cur", default=None, destroy=True)
            if len(largs) + len(kargs) == 0:
                return fun(self, store=store, cur=cur)
            else:
                return type.__getattribute__(this.cl,
                                             fun.func_name)(self,
                                                            *largs, **kargs)
        decorated.func_name = fun.func_name
        decorated.__doc__ = fun.__doc__
        return this.documented(decorated)

    def determined(this, *lattrs, **attrs):
        r"""
        Determine the value of a property if a condition is satisfied.

        The decorated function should return a pair ``(upd, ats)``, where
        ``upd`` is a boolean signifying whether the computed value should be
        updated, and ``ats`` is a dictionary whose keys are names of the
        properties that should be updated to whether the corresponding value
        matches the computed value of the attribute (note that any non-string
        keys will be ignored). The function should accept the following
        parameters:

        - ``value`` - the value computed by the overridden function.

        - ``attrs`` - a dictionary mapping boolean atributes or expressions
          to the values of the sought attribute that they imply if true.

        - ``store`` - whether to store the computed results back to the
          database (named parameter).

        - ``cur`` - the cursor to use for database interaction (named
          parameter).

        INPUT:

        - an unnamed attribute should be a tuple whose first element is an
          attribute name or an expressions, and the second element is the value
          of the sought attribute that is implied when the value of the
          attribute or expression represented by the first element is true.

        - a named attribute is equivalent to a tuple containing its name and
          value.
        """
        attrs.update(lattrs)

        def _determined(fun):
            def decorated(self, *largs, **kargs):
                store = lookup(kargs, "store",
                               default=discretezoo.WRITE_TO_DB,
                               destroy=True)
                cur = lookup(kargs, "cur", default=None, destroy=True)
                default = len(largs) + len(kargs) == 0
                d = self._getprops(fun.func_name)
                try:
                    if not default:
                        raise NotImplementedError
                    for k, v in attrs.items():
                        try:
                            if parse(self, k):
                                return v
                        except KeyError:
                            pass
                    return lookup(d, fun.func_name)
                except (KeyError, NotImplementedError):
                    a = type.__getattribute__(this.cl,
                                              fun.func_name)(self,
                                                             *largs, **kargs)
                    if default:
                        upd, ats = fun(self, a, dict(attrs), store=store,
                                       cur=cur)
                        if store:
                            t = {}
                            if upd:
                                t[self._getclass(fun.func_name)] = \
                                                            {fun.func_name: a}
                            for k, v in ats.items():
                                if not isinstance(k, basestring):
                                    continue
                                cl = self._getclass(k)
                                if cl not in t:
                                    t[cl] = {}
                                t[cl][k] = a == v
                            for cl, at in t.items():
                                self._update_rows(cl, at,
                                                  {self._spec["primary_key"]:
                                                   self._zooid}, cur=cur)
                        if upd:
                            update(d, fun.func_name, a)
                        for k, v in ats.items():
                            if isinstance(k, basestring):
                                update(self._getprops(k), k, a == v)
                    return a
            decorated.func_name = fun.func_name
            decorated.__doc__ = fun.__doc__
            return this.documented(decorated)
        return _determined
